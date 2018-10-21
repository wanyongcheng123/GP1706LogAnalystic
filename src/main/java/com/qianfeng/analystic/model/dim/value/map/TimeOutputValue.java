package com.qianfeng.analystic.model.dim.value.map;

import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//用户模块和浏览器模块的map阶段的value输出阶段
public class TimeOutputValue extends OutputValueBaseWritable {
    private String id;  //泛指 uuid sesionid  memberid
    private long time; //时间

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}