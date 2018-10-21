package com.qianfeng.analystic.mr;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.analystic.mr.service.imp.IDimensionConvertImpl;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiType;
import com.qianfeng.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Auther:
 * @Date: 2018/7/30 10:00
 * @Description:自定义输出到mysql的输出格式类
 */
public class IOutputWritterFormat extends OutputFormat<BaseDimension,OutputValueBaseWritable>{
    private static final Logger logger = Logger.getLogger(IOutputWritterFormat.class);

    @Override
    public RecordWriter<BaseDimension, OutputValueBaseWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Connection conn = JdbcUtil.getConn();
        IDimensionConvert convert = new IDimensionConvertImpl();
        return new OutputWritterRecordWritter(conn,conf,convert);
    }


    /**
     * 自定封装writter的类
     */
    public static class OutputWritterRecordWritter extends RecordWriter<BaseDimension,OutputValueBaseWritable>{
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConvert convert = null;
        //用于判断kpi的数据量
        private Map<KpiType,Integer> batch = new HashMap<KpiType,Integer>();
        //用于存储kpi对应的ps，方便下一次直接获取
        private Map<KpiType,PreparedStatement> map = new HashMap<KpiType,PreparedStatement>();

        public OutputWritterRecordWritter(Connection conn, Configuration conf, IDimensionConvert convert) {
            this.conn = conn;
            this.conf = conf;
            this.convert = convert;
        }

        /**
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(BaseDimension key, OutputValueBaseWritable value) throws IOException, InterruptedException {
            if(key == null || value == null){
                return;
            }
            //key 和 value不能空
            //1、获取kpi,然后根据kpi获取对应的sql。
            KpiType kpi = value.getKpi();
            PreparedStatement ps = null;
            try{
                int count = 1;  //批量的初始值
                if(map.get(kpi) == null){
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi,ps);
                } else {
                    ps = map.get(kpi);
                    count = batch.get(kpi);
                    count ++;
                }
                //将批量的值更新到batch中
                this.batch.put(kpi,count);


                //2、为ps赋值  output_new_user
                String outputWritterName = conf.get(GlobalConstants.PREFIX_OUTPUT+kpi.kpiName);
                Class classz = Class.forName(outputWritterName);
                IOutputWritter outputWritter = (IOutputWritter) classz.newInstance();
                outputWritter.outputWrite(conf,key,value,ps,convert);  //调用接口

                //判断有多少个ps
                if(count % GlobalConstants.NUM_OF_BATCH == 0){
                    ps.addBatch();
                    conn.commit();
                    //执行完成移除
                    batch.remove(kpi);
                }

            } catch (Exception e){
                logger.warn("执行存储结果到mysql异常.",e);
            }
        }

        /**
         * 关闭,确保剩余的ps被执行以遍
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType,PreparedStatement> en:map.entrySet()){
                    en.getValue().executeBatch();
                }
            } catch (SQLException e) {
                logger.warn("关闭对象时，执行sql异常",e);
            }finally {
                try {
                    for (Map.Entry<KpiType,PreparedStatement> en:map.entrySet()){
                        en.getValue().close();
                        map.remove(en.getKey()); //移除
                    }
                } catch (SQLException e) {
                    logger.warn("关闭时ps的时候异常.",e);
                } finally {
                    JdbcUtil.close(conn,null,null);  //关闭conn
                }
            }
        }
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        //不用检测输出空间
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null,context);
    }
}
