package com.qianfeng.etl.mr.tohbase;

import com.qianfeng.common.EventLogsConstant;
import com.qianfeng.etl.util.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/*
将解析后的数据存储到hbase中的mapper类
 */
public class LogToHbaseM extends Mapper<Object,Text,NullWritable,Put> {
    private static final Logger logger = Logger.getLogger(LogToHbaseM.class);
    private byte[] family = Bytes.toBytes(EventLogsConstant.HBASE_COLUMN_FAMILY);
    //输入和输出和过滤行记录
    private int inputRecords, outputRecords, filterRecords = 0;
    private CRC32 crc32 = new CRC32();


    @Override
   protected void map(Object key, Text value, Mapper.Context context) throws IOException {
        this.inputRecords ++ ;
        logger.info("输入的日志为:" + value.toString());
        Map<String,String> info =new LogUtil().parserLog(value.toString());
        if (info.isEmpty()){
            this.filterRecords ++;
            return;
        }
        //获取事件
        String eventName =info.get(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_NAME);
        EventLogsConstant.EventEnum event =EventLogsConstant.EventEnum.valueOf(eventName);
        switch (event){
            case EVENT:
            case PAGEVIEW:
            case CHARGEREFUND:
            case CHARGEREQUEST:
            case CHARGESUCCESS:
            case LAUNCH:
                //将info存储
                handleInfo(info,eventName,context);
                break;
                default:
                    filterRecords ++;
                    logger.warn("该事件暂时不支持数据清洗.event:"+eventName);
                    break;

        }

    }
    /*
    写数据到hbase中,根据日期获取到hbase中某张表的该日期所在的天的数据??
     */


    private void handleInfo(Map<String, String> info, String eventName, Context context) {
        try {
        if (!info.isEmpty()) {
            //去出uuid s_time u_mid来构建row_key
            String uuid = info.get(EventLogsConstant.EVENT_COLUMN_NAME_UUID);
            String serverTime = info.get(EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME);
            String memberId = info.get(EventLogsConstant.EVENT_COLUMN_NAME_MEMBER_ID);
            //构建rowkey
            String rowkey = buildRowkey(uuid, serverTime, memberId, eventName);
            Put put = new Put(Bytes.toBytes(rowkey));
            for (Map.Entry<String, String> en : info.entrySet()) {
                //将k-v添加到put
                put.addColumn(family, Bytes.toBytes(en.getKey()), Bytes.toBytes(en.getValue()));

            }
            //输出
            context.write(NullWritable.get(), put);
            this.outputRecords++;
        }
            } catch (Exception e) {
            this.filterRecords ++;
            logger.warn("写出到hbase异常",e);

            }
        }

    private String buildRowkey(String uuid, String serverTime, String memberId, String eventName) {
    StringBuffer sb =new StringBuffer();
    sb.append(serverTime+ "_");
    //需要将才crc32初始化
        crc32.reset();
        if (StringUtils.isNotEmpty(uuid)){
            this.crc32.update(uuid.getBytes());
        }
        if (StringUtils.isNotEmpty(memberId)){
            this.crc32.update(memberId.getBytes());
        }
        if (StringUtils.isNotEmpty(eventName)){
            this.crc32.update(eventName.getBytes());
        }
        sb.append(this.crc32.getValue()%10000000L);
        return  sb.toString();

    }
   @Override
    protected void cleanup (Context context)throws IOException{
        logger.info("输入,输出和过滤的记录数.inputRecords"+ this.inputRecords
        +"outputRecords:" + this.outputRecords + "filterRecords:" + this.filterRecords);

   }



}
