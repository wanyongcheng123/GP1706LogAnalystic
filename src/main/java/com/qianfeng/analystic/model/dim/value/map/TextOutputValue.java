package com.qianfeng.analystic.model.dim.value.map;

import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 15:04
 * @Description:地域模块的map阶段的value输出类型
 */
public class TextOutputValue extends OutputValueBaseWritable{
    private String uuid = "";
    private String sessionId = "";

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uuid);
        out.writeUTF(this.sessionId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = in.readUTF();
        this.sessionId = in.readUTF();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
