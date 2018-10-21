package com.qianfeng.etl.mr.tohdfs;



import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @Auther:
 * @Date: 2018/7/26 11:49
 * @Description:驱动类
 */
public class LogToHdfsRunner implements Tool{
    private static final Logger logger = Logger.getLogger(LogToHdfsRunner.class);
    Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new LogToHdfsRunner(),args);
        } catch (Exception e) {
            logger.warn("运行etl to hdfs 异常",e);
        }
    }

    @Override
    public void setConf(Configuration conf) {

        this.conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * yarn jar /home/gp1706.jar com.qianfeng.etl.mr.tohbase.ToLogRunner -d 2018-07-26
     * @param args   /logs/07/26
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        //设置处理的参数
        this.setArgs(args,conf);

        //获取job
        Job job = Job.getInstance(conf,"to hdfs etl");
        job.setJarByClass(LogToHdfsRunner.class);

        //设置map端的属性
        job.setMapperClass(LogToHdfsMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LogDataWritable.class);


        job.setNumReduceTasks(0);

        //设置map阶段的输入路径
        this.setInputOutputPath(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 设置输入输出的路径
     * @param job
     */
    private void setInputOutputPath(Job job) {
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String fields[] = date.split("-");
        Path inputPath = new Path("/logs/"+fields[1]+"/"+fields[2]);
        Path outputPath = new Path("/ods/month="+fields[1]+"/day="+fields[2]);
        try {
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(inputPath)){
                FileInputFormat.addInputPath(job,inputPath);
            } else {
                throw new RuntimeException("数据数据路径不存在.");
            }
            if(fs.exists(outputPath)){
                fs.delete(outputPath,true);
            }
            FileOutputFormat.setOutputPath(job,outputPath);
        } catch (IOException e) {
            logger.warn("获取fs对象异常.",e);
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

}
