package com.qianfeng.analystic.mr.nm;

import com.google.common.collect.Lists;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import com.qianfeng.analystic.mr.IOutputWritterFormat;
import com.qianfeng.analystic.mr.am.ActiveMemberMapper;
import com.qianfeng.analystic.mr.am.ActiveMemberReudcer;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.analystic.mr.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.EventLogsConstant;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.JdbcUtil;
import com.qianfeng.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Auther: lyd
 * @Date: 2018/7/30 14:40
 * @Description:新增会员的runner类
 */
public class NewMemberRunner implements Tool{
    private static final Logger logger = Logger.getLogger(NewMemberRunner.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new NewMemberRunner(),args);
        } catch (Exception e) {
            logger.error("运行会员会员指标失败.",e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf.addResource("query-mapping.xml");
        this.conf.addResource("output-writter.xml");
        this.conf.addResource("total-mapping.xml");
        this.conf = HBaseConfiguration.create(this.conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        //设置参数到conf中
        this.setArgs(args,conf);
        //获取作业
        Job job = Job.getInstance(conf,"new_member");
        job.setJarByClass(NewMemberRunner.class);

        //初始化mapper类
        //addDependencyJars : true则是本地提交集群运行，false是本地提交本地运行
        TableMapReduceUtil.initTableMapperJob(this.getScans(job),NewMemberMapper.class,
                StatsUserDimension.class, TimeOutputValue.class,job,false);

        //reducer的设置
        job.setReducerClass(NewMemberReudcer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        //设置输出的格式类
        job.setOutputFormatClass(IOutputWritterFormat.class);

//        return job.waitForCompletion(true)?0:1;
        if(job.waitForCompletion(true)){
            computeNewTotalMember(job);
            return 0;
        } else {
            return 1;
        }
    }

    private void computeNewTotalMember(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取运行的日期
            String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
            long nowDay = TimeUtil.parserString2Long(date);
            long yesterDay = nowDay - GlobalConstants.DAY_OF_MILISECONDS;

            //获取对应的时间维度
            DateDimension nowDateDimension = DateDimension.buildDate(nowDay, DateEnum.DAY);
            DateDimension yesterdayDateDimension = DateDimension.buildDate(yesterDay, DateEnum.DAY);

            int nowDimensionId = -1;
            int yesterDimensionId = -1;

            //获取维度的id
            IDimensionConvert convert = new IDimensionConvertImpl();
            nowDimensionId = convert.getDimensionIdByDimension(nowDateDimension);
            yesterDimensionId = convert.getDimensionIdByDimension(yesterdayDateDimension);

            //判断昨天和今天的维度Id是否大于0
            conn = JdbcUtil.getConn();
            Map<String,Integer> map = new HashMap<String,Integer>();
            if(yesterDimensionId > 0){
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL+"new_total_member"));
                //赋值
                ps.setInt(1,yesterDimensionId);
                //执行
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalNewUser = rs.getInt("total_members");
                    //存储
                    map.put(platformId+"",totalNewUser);
                }
            }

            if(nowDimensionId > 0){
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL+"user_new_member"));
                //赋值
                ps.setInt(1,nowDimensionId);
                //执行
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUser = rs.getInt("new_members");
                    //存储
                    if(map.containsKey(platformId+"")){
                        newUser += map.get(platformId+"");
                    }
                    map.put(platformId+"",newUser);
                }
            }

            //更新新增的总用户
            ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL+"user_new_update_member"));
            //赋值
            for (Map.Entry<String,Integer> en:map.entrySet()){
                ps.setInt(1,nowDimensionId);
                ps.setInt(2,Integer.parseInt(en.getKey()));
                ps.setInt(3,en.getValue());
                ps.setString(4,conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(5,en.getValue());
                ps.execute();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn,ps,rs);
        }
    }


    /**
     * 参数处理  ,将接收到的日期存储在conf中，以供后续使用
     * @param args  如果没有传递日期，则默认使用昨天的日期
     * @param conf
     */
    private void setArgs(String[] args, Configuration conf) {
        String date = null;
        for (int i = 0;i < args.length;i++){
            if(args[i].equals("-d")){
                if(i+1 < args.length){
                    date = args[i+1];
                    break;
                }
            }
        }
        //代码到这儿，date还是null，默认用昨天的时间
        if(date == null){
            date = TimeUtil.getYesterdayDate();
        }
        //然后将date设置到时间conf中
        conf.set(GlobalConstants.RUNNING_DATE,date);
    }

    /**
     * 获取扫描的集合对象
     * @param job
     * @return
     */
    private List<Scan> getScans(Job job) {
        Configuration conf = job.getConfiguration();
        //获取运行日期
        long start = Long.valueOf(TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE)));
        long end = start + GlobalConstants.DAY_OF_MILISECONDS;
        //获取scan对象
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start+""));
        scan.setStopRow(Bytes.toBytes(end+""));
        //定义过滤器链
        FilterList fl = new FilterList();

        //设置扫描的字段
        String[] fields = {
          EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME,
          EventLogsConstant.EVENT_COLUMN_NAME_MEMBER_ID,
          EventLogsConstant.EVENT_COLUMN_NAME_PLATFORM,
          EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_NAME,
          EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_VERSION
        };
        //将扫描的字段添加到filter中
        fl.addFilter(this.getFilters(fields));
        //将scan设置表名
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes(EventLogsConstant.HABSE_TABLE_NAME));
        //将filter设置到scan中
        scan.setFilter(fl);
        return Lists.newArrayList(scan);  //谷歌的api
    }

    /**
     * 设置扫描的列
     * @param fields
     * @return
     */
    private Filter getFilters(String[] fields) {
        int length = fields.length;
        byte[][] filters = new byte[length][];
        for (int i=0;i<length;i++){
            filters[i] = Bytes.toBytes(fields[i]);
        }
        return new MultipleColumnPrefixFilter(filters);
    }
}
