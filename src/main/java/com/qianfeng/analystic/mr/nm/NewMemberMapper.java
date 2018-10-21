package com.qianfeng.analystic.mr.nm;

import com.qianfeng.analystic.model.dim.StatsCommonDimension;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.EventLogsConstant;
import com.qianfeng.common.KpiType;
import com.qianfeng.util.JdbcUtil;
import com.qianfeng.util.MemberUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 10:27
 * @Description: 新增的会员
 */
public class NewMemberMapper extends TableMapper<StatsUserDimension,TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
    private byte[] family = Bytes.toBytes(EventLogsConstant.HBASE_COLUMN_FAMILY);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension newMemberKpi = new KpiDimension(KpiType.NEW_MEMBER.kpiName);
    private KpiDimension browserNewemberKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.kpiName);
    private Connection conn = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conn = JdbcUtil.getConn();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取需要的字段
        String memberId = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_MEMBER_ID)));
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_VERSION)));

        //对三个字段进行空判断
        if(StringUtils.isEmpty(memberId) || StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(platform)){
            logger.warn("uuid&&serverTime&&platform must not null.memberId:"+memberId
                    +"  serverTime:"+serverTime+"  platform:"+platform);
            return;
        }

        //判断会员ID是否是新会员
        if(!MemberUtil.checkMemberId(memberId)){
            logger.info("memberId is invalid.memberId:"+memberId);
            return;
        }

        if(!MemberUtil.isNewMember(memberId,conn,context.getConfiguration())){
            logger.info("memberId is not new memberId.memberId:"+memberId);
            return;
        }

        //构建输出value
        long serverTimeOfLong = Long.valueOf(serverTime);
        this.v.setId(memberId);
        this.v.setTime(serverTimeOfLong);


        //构建输出的key
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        DateDimension dateDimension = DateDimension.buildDate(serverTimeOfLong, DateEnum.DAY);
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName,browserVersion);


        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //为statsCommonDimension赋值
        statsCommonDimension.setDateDimension(dateDimension);

        BrowserDimension defaultBrowser = new BrowserDimension("","");

        //循环平台维度集合对象
        for (PlatformDimension pl:platformDimensions) {
            statsCommonDimension.setKpiDimension(newMemberKpi);
            statsCommonDimension.setPlatformDimension(pl);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.k.setBrowserDimension(defaultBrowser);
            //输出
            context.write(this.k,this.v);
            //该循环的输出用于浏览器模块的新增用户指标统计
            for (BrowserDimension br:browserDimensionList){
                statsCommonDimension.setKpiDimension(browserNewemberKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                this.k.setBrowserDimension(br);
                //写出
                context.write(this.k,this.v);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        JdbcUtil.close(conn,null,null);
    }
}
