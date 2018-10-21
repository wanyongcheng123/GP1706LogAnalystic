package com.qianfeng.analystic.model.dim.value.reduce;

import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 15:16
 * @Description:地域模块reduce阶段下的value的输出类型
 */
public class LocalOutputValue extends OutputValueBaseWritable{
    private int aus;
    private int sessions;
    private int boundSessions;  //跳出会话个数
    private KpiType kpi;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.aus);
        out.writeInt(this.sessions);
        out.writeInt(this.boundSessions);
        WritableUtils.writeEnum(out,kpi); //注意枚举写出
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.aus = in.readInt();
        this.sessions = in.readInt();
        this.boundSessions = in.readInt();
        WritableUtils.readEnum(in,KpiType.class);
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    public int getAus() {
        return aus;
    }

    public void setAus(int aus) {
        this.aus = aus;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBoundSessions() {
        return boundSessions;
    }

    public void setBoundSessions(int boundSessions) {
        this.boundSessions = boundSessions;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }
}
