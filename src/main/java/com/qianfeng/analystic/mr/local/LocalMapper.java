package com.qianfeng.analystic.mr.local;

import com.qianfeng.analystic.model.dim.StatsCommonDimension;
import com.qianfeng.analystic.model.dim.StatsLocalDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.LocationDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.model.dim.value.map.TextOutputValue;
import com.qianfeng.analystic.mr.am.ActiveMemberMapper;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.EventLogsConstant;
import com.qianfeng.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @Auther: lyd
 * @Date: 2018/8/2 11:46
 * @Description:
 */
public class LocalMapper extends TableMapper<StatsLocalDimension,TextOutputValue>{
    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private byte[] family = Bytes.toBytes(EventLogsConstant.HBASE_COLUMN_FAMILY);
    private StatsLocalDimension k = new StatsLocalDimension();
    private TextOutputValue v = new TextOutputValue();
    private KpiDimension localKpi = new KpiDimension(KpiType.LOCAL.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取需要的字段
        String uuid = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_UUID)));
        String sessionId = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_SESSION_ID)));
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_PLATFORM)));
        String country = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_COUNTRY)));
        String province = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_PROVINCE)));
        String city = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_CITY)));


        //对三个字段进行空判断
        if(StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(platform)){
            logger.warn("uuid&&serverTime&&platform must not null.memberId:"
                    +"  serverTime:"+serverTime+"  platform:"+platform);
            return;
        }

        if(StringUtils.isEmpty(uuid)){
            uuid = "";
        }

        if(StringUtils.isEmpty(sessionId)){
            sessionId = "";
        }

        //构建输出value
        long serverTimeOfLong = Long.valueOf(serverTime);
        this.v.setUuid(uuid);
        this.v.setSessionId(sessionId);

        //构建输出的key
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        DateDimension dateDimension = DateDimension.buildDate(serverTimeOfLong, DateEnum.DAY);
        List<LocationDimension> locationDimensionList = LocationDimension.buildList(country,province,city);

        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //为statsCommonDimension赋值
        statsCommonDimension.setDateDimension(dateDimension);

        //循环平台维度集合对象
        for (PlatformDimension pl:platformDimensions) {

            statsCommonDimension.setPlatformDimension(pl);

            //该循环的输出用于浏览器模块的新增用户指标统计
            for (LocationDimension local:locationDimensionList){
                statsCommonDimension.setKpiDimension(localKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                this.k.setLocationDimension(local);
                //写出
                context.write(this.k,this.v);
            }
        }
    }
}
