package com.qianfeng.etl.mr.tohdfs;


import com.qianfeng.common.EventLogsConstant;

import com.qianfeng.etl.util.LogUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * @Auther:
 * @Date: 2018/7/26 11:11
 * @Description:将解析后的数据存储到hdfs中的mapper类
 */
public class LogToHdfsMapper extends Mapper<Object,Text,NullWritable,LogDataWritable> {
    private static final Logger logger = Logger.getLogger(LogToHdfsMapper.class);
    private byte[] family = Bytes.toBytes(EventLogsConstant.HBASE_COLUMN_FAMILY);
    //输入输出和过滤行记录
    private int inputRecords,outputRecords,filterRecords = 0;


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords ++;
        logger.info("输入的日志为:"+value.toString());
        Map<String,String> info = new LogUtil().parserLog(value.toString());
        if(info.isEmpty()){
            this.filterRecords ++;
            return;
        }
        //获取事件
        String eventName = info.get(EventLogsConstant.EVENT_COLUMN_NAME_EVENT_NAME);
        EventLogsConstant.EventEnum event = EventLogsConstant.EventEnum.valueOfAlias(eventName);
        switch (event){
            case EVENT:
            case LAUNCH:
            case PAGEVIEW:
            case CHARGEREQUEST:
            case CHARGESUCCESS:
            case CHARGEREFUND:
                //将info存储
                handleInfo(info,eventName,context);
                break;
            default:
                filterRecords ++;
                logger.warn("该事件暂时不支持数据的清洗.event:"+eventName);
                break;
        }
    }


    /**
     * 写数据到hdfs
     * @param info
     * @param eventName
     * @param context
     */
    private void handleInfo(Map<String, String> info, String eventName, Context context) {
        try {
            if(!info.isEmpty()){
                LogDataWritable ld = new LogDataWritable();
                ld.s_time  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME);
                ld.en  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_EVENT_NAME);
                ld.ver  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_VERSION);
                ld.u_ud  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_UUID);
                ld.u_mid  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_MEMBER_ID);
                ld.u_sid  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_SESSION_ID);
                ld.c_time  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_CLIENT_TIME);
                ld.language  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_LANGUAGE);
                ld.b_iev  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_USERAGENT);
                ld.b_rst  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_RESOLUTION);
                ld.p_url  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_CURRENT_URL);
                ld.p_ref  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_PREFFER_URL);
                ld.tt  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_TITLE);
                ld.pl  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_PLATFORM);
                ld.oid  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_ORDER_ID);
                ld.on  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_ORDER_NAME);
                ld.cut  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_CURRENCY_TYPE);
                ld.cua  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_CURRENCY_AMOUTN);
                ld.pt  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_PAYMENT_TYPE);
                ld.ca  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_EVENT_CATEGORY);
                ld.ac  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_EVENT_ACTION);
                ld.kv_  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_EVENT_KV);
                ld.du  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_EVENT_DURATION);
                ld.os  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_OS_NAME);
                ld.os_v  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_OS_VERSION);
                ld.browser  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_NAME);
                ld.browser_v  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_VERSION);
                ld.country  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_COUNTRY);
                ld.province  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_PROVINCE);
                ld.city  =info.get(EventLogsConstant.EVENT_COLUMN_NAME_CITY);
                //输出
                context.write(NullWritable.get(),ld);
                this.outputRecords ++;
            }
        } catch (Exception e) {
            this.filterRecords ++;
            logger.warn("写出到hbase异常.",e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("输入、输出和过滤的记录数.inputRecords"+this.inputRecords
                +"  outputRecords:"+this.outputRecords+"  filterRecords:"+this.filterRecords);
    }
}
