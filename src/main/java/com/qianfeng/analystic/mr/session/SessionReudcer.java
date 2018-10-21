package com.qianfeng.analystic.mr.session;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 15:13
 * @Description:session的reducer
 */
public class SessionReudcer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,MapWritableValue>{
    private MapWritableValue v = new MapWritableValue();

    /**
     * 2018-07-26 website 111 123
     * 2018-07-26 website 111 125
     * 2018-07-26 website 112 133
     *
     * 2018-07-26 website list((111,123),(111,125),(111,100))
     *111-list(123,125,110,111)
     */
    private Map<String,List<Long>> map = new HashMap<String,List<Long>>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        this.map.clear();
        //循环map阶段传过来的value
        for (TimeOutputValue tv:values) {
            String sessionId = tv.getId();
            long serverTime = tv.getTime();
            //存储时间
            if(map.containsKey(tv.getId())){
                List<Long> li = map.get(sessionId);
                li.add(serverTime);
                map.put(sessionId,li);
            } else {
                List<Long> li = new ArrayList<Long>();
                li.add(serverTime);
                map.put(sessionId,li);
            }
        }

        //构建输出的value
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1),new IntWritable(this.map.size()));
        //session的时长
        int sesssionLength = 0;
        for (Map.Entry<String,List<Long>> en:map.entrySet()) {
            if(en.getValue().size() >= 2){
                Collections.sort(en.getValue());
                sesssionLength += (en.getValue().get(en.getValue().size()-1) - en.getValue().get(0));
                System.out.println(en.getKey()+"aaaaaa:"+sesssionLength);
            }
        }
        System.out.println("aaaaaa:"+sesssionLength);

        if(sesssionLength > 0 && sesssionLength <= GlobalConstants.DAY_OF_MILISECONDS){
            //不足一秒算一秒
            if(sesssionLength%1000 == 0){
                sesssionLength = sesssionLength / 1000;
            } else {
                sesssionLength = sesssionLength / 1000 + 1;
            }
        }
        mapWritable.put(new IntWritable(-2),new IntWritable(sesssionLength));
        this.v.setValue(mapWritable);
        //还需要设置kpi
       this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //输出即可
        context.write(key,this.v);
    }
}
