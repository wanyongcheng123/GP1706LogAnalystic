package com.qianfeng.analystic.mr.pv;

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
 * @Date: 2018/7/27 10:27
 * @Description: pv的mapper
 */
public class PageviewMapper extends TableMapper<StatsUserDimension,TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(PageviewMapper.class);
    private byte[] family = Bytes.toBytes(EventLogsConstant.HBASE_COLUMN_FAMILY);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension pageviewKpi = new KpiDimension(KpiType.PAGEVIEW.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取需要的字段
        String url = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_CURRENT_URL)));
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_VERSION)));


        //对三个字段进行空判断
        if(StringUtils.isEmpty(url) || StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(platform)){
            logger.warn("url&&serverTime&&platform must not null.url:"+url
                    +"  serverTime:"+serverTime+"  platform:"+platform);
            return;
        }

        //构建输出value
        long serverTimeOfLong = Long.valueOf(serverTime);
        this.v.setId(url);
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

            statsCommonDimension.setPlatformDimension(pl);

            //该循环的输出用于浏览器模块的新增用户指标统计
            for (BrowserDimension br:browserDimensionList){
                statsCommonDimension.setKpiDimension(pageviewKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                this.k.setBrowserDimension(br);
                //写出
                context.write(this.k,this.v);
            }
        }
    }
}
