package com.qianfeng.analystic.mr.am;

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
 * @Description: 活跃的会员和活跃的总会员统计的mapper类,根据pageview事件的数据中memberid的去重个数
 */
public class ActiveMemberMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private byte[] family = Bytes.toBytes(EventLogsConstant.HBASE_COLUMN_FAMILY);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension ActiveMemberKpi = new KpiDimension(KpiType.ACTIVE_MEMBER.kpiName);
    private KpiDimension browserActiveMemberKpi =new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.kpiName);
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取需要的字段
        String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_UUID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_VERSION)));
        //对三个字段进行空判断
        if (StringUtils.isEmpty(uuid) || StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(platform)) {
            logger.warn("uuid&&serverTime&&platfrom must not null.uuid:" + uuid
                    + " serverTime:" + serverTime + "platfrom:" + platform);
            return;
        }
        //构建输出value
        Long serverTimeOfLong = Long.valueOf(serverTime);
        this.v.setId(uuid);
        this.v.setTime(serverTimeOfLong);
        /**
         * 1532593870123 2018-07-26 website 27F69684-BBE3-42FA-AA62-71F98E208
         *
         */
        //构建输出的key
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        DateDimension dateDimension = DateDimension.buildDate(serverTimeOfLong, DateEnum.DAY);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        List<BrowserDimension> browserDimensionList =BrowserDimension.buildList(browserName,browserVersion);
        //为statsCommonDimension赋值
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setKpiDimension(ActiveMemberKpi);
        BrowserDimension defaultBrowser = new BrowserDimension("", "");
        //循环平台维度集合对象
        for (PlatformDimension pl : platformDimensions) {
            statsCommonDimension.setPlatformDimension(pl);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.k.setBrowserDimension(defaultBrowser);
            //输出
            context.write(this.k, this.v);
            /**
             *
             * datedimension platform kpi   v
             * 1    2   new_user     1532593870123  27F69684-BBE3-42FA-AA62-71F98E208
             * 1    1   new_user     1532593870123  27F69684-BBE3-42FA-AA62-71F98E208
             *
             */
            //该循环的输出用于浏览器模块的新增用户指标统计
            for(BrowserDimension br: browserDimensionList){
                statsCommonDimension.setKpiDimension(browserActiveMemberKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                this.k.setBrowserDimension(br);
                //写出
                context.write(this.k,this.v);

            }
        }


    }
}
