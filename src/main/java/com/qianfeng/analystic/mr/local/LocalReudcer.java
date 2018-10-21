package com.qianfeng.analystic.mr.local;

import com.qianfeng.analystic.model.dim.StatsLocalDimension;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TextOutputValue;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.LocalOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import com.qianfeng.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 15:13
 * @Description:地域的的reducer
 */
public class LocalReudcer extends Reducer<StatsLocalDimension,TextOutputValue,
        StatsLocalDimension,LocalOutputValue>{
    private Set<String> unique = new HashSet<String>(); //uuid的个数
    private Map<String,Integer> map = new HashMap<String,Integer>();
    private LocalOutputValue v = new LocalOutputValue();

    @Override
    protected void reduce(StatsLocalDimension key, Iterable<TextOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空unique
        this.unique.clear();
        this.map.clear();
        //循环map阶段传过来的value
        for (TextOutputValue tv:values) {
            //将uuid取出来添加到set中
            if(StringUtils.isNotEmpty(tv.getUuid())){
                this.unique.add(tv.getUuid());
            }

            if(StringUtils.isNotEmpty(tv.getSessionId())){
                if(map.containsKey(tv.getSessionId())){
                    map.put(tv.getSessionId(),2);
                } else {
                    map.put(tv.getSessionId(),1);
                }
            }
        }

        //构建输出的value
        this.v.setAus(this.unique.size());
        this.v.setSessions(this.map.size());
        int boundSessions = 0;
        //循环
        for (Map.Entry<String,Integer> en:map.entrySet()){
            if(en.getValue() < 2){
                boundSessions ++;
            }
        }
        this.v.setBoundSessions(boundSessions);
        //还需要设置kpi
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //输出即可
        context.write(key,this.v);
    }
}
