package com.qianfeng.analystic.model.dim.value.reduce;

import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther:
 * @Date: 2018/7/27 15:16
 * @Description:用户模块和浏览器模块reduce阶段下的value的输出类型
 */
public class MapWritableValue extends OutputValueBaseWritable {
    private MapWritable value = new MapWritable();
    private KpiType kpi;

    @Override
    public void write(DataOutput out) throws IOException {
        this.value.write(out);  //mapWritable的写出
        WritableUtils.writeEnum(out,kpi); //注意枚举写出
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        WritableUtils.readEnum(in,KpiType.class);
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }
}
