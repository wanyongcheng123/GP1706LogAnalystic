package com.qianfeng.analystic.mr.am;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @Auther:
 * @Date: 2018/7/27 15:13
 * @Description:
 */
//新增用户的reduce类
public class ActiveMemberReudcer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,MapWritableValue>{
        private Set<String>  unique =new HashSet<String>();
        private  MapWritableValue v =new MapWritableValue();

       // PlatformDimension platformDimension = new PlatformDimension();

        @Override
        protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
                super.reduce(key, values, context);
                this.unique.clear();
                /**
                 *
                 * datedimension platform kpi   v
                 * 1    2   new_user     list(1532593870123  27F69684-BBE3-42FA-AA62-71F98E208)
                 * 1    1   new_user     list(1532593870123  27F69684-BBE3-42FA-AA62-71F98E208)
                 */
                //循环map阶段传过来的value
                for(TimeOutputValue tv :values){
                        //将uuid取出来添加到set中
                        this.unique.add(tv.getId());
                }
                //构造输出的value
                MapWritable mapWritable =new MapWritable();
                mapWritable.put(new IntWritable(-1),new IntWritable(this.unique.size()));
                this.v.setValue(mapWritable);
                //还需要设置kpi
                if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_MEMBER.kpiName)){
                        this.v.setKpi(KpiType.ACTIVE_MEMBER);
                }
                //输出即可
                context.write(key,this.v);
        }
}
