package com.qianfeng.analystic.mr.nm;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 15:13
 * @Description:新增的会员的reducer
 */
public class NewMemberReudcer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,MapWritableValue>{
    private Set<String> unique = new HashSet<String>();
    private MapWritableValue v = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空unique
        this.unique.clear();
        //循环map阶段传过来的value
        for (TimeOutputValue tv:values) {
            //将uuid取出来添加到set中
            this.unique.add(tv.getId());
        }
        MapWritable mapWritable = new MapWritable();
        //循环unique
        for (String id:this.unique){
            this.v.setKpi(KpiType.MEMBER_INFO);
            mapWritable.put(new IntWritable(-1),new Text(id));
            this.v.setValue(mapWritable);
            context.write(key,this.v);
        }

        //构建输出的value

        mapWritable.put(new IntWritable(-1),new IntWritable(this.unique.size()));

        this.v.setValue(mapWritable);
        //还需要设置kpi
       this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //输出即可
        context.write(key,this.v);

    }
}
