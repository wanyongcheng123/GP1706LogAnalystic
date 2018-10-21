package com.qianfeng.analystic.model.dim;

import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.model.dim.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//公共维度类的封装 平台和时间维度
public class StatsCommonDimension extends StatsBaseDimension {
    private PlatformDimension platfromDimension =new PlatformDimension();
    private DateDimension dateDimension =new DateDimension();
    private KpiDimension kpiDimension =new KpiDimension();
public StatsCommonDimension(){

}
public StatsCommonDimension(PlatformDimension platfromDimension, DateDimension dateDimension, KpiDimension kpiDimension){
    this.platfromDimension =platfromDimension;
    this.dateDimension=dateDimension;
    this.kpiDimension= kpiDimension;
}

//克隆当前对象的一个实例,dimension
public static StatsCommonDimension clone(StatsCommonDimension dimension){
    DateDimension dateDimension = new DateDimension(dimension.dateDimension.getId(),
            dimension.dateDimension.getYear(),dimension.dateDimension.getSeason(),
            dimension.dateDimension.getMonth(),dimension.dateDimension.getWeek(),
            dimension.dateDimension.getDay(),dimension.dateDimension.getCalendar(),
            dimension.dateDimension.getType());
    PlatformDimension platformDimension = new PlatformDimension(dimension.platfromDimension.getId(),
            dimension.platfromDimension.getPlatformName());
    KpiDimension kpiDimension = new KpiDimension(dimension.kpiDimension.getId(),
            dimension.kpiDimension.getKpiName());
    return new StatsCommonDimension(platformDimension,dateDimension,kpiDimension);
}

    @Override
    public void write(DataOutput out) throws IOException {
        this.dateDimension.write(out); //对象的写出
        this.platfromDimension.write(out);
        this.kpiDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dateDimension.readFields(in);
        this.platfromDimension.readFields(in);
        this.kpiDimension.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {

        if(o == this){
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.platfromDimension.compareTo(other.platfromDimension);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.kpiDimension.compareTo(other.kpiDimension);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsCommonDimension that = (StatsCommonDimension) o;

        if (platfromDimension != null ? !platfromDimension.equals(that.platfromDimension) : that.platfromDimension != null)
            return false;
        if (dateDimension != null ? !dateDimension.equals(that.dateDimension) : that.dateDimension != null)
            return false;
        return kpiDimension != null ? kpiDimension.equals(that.kpiDimension) : that.kpiDimension == null;
    }

    @Override
    public int hashCode() {
        int result = platfromDimension != null ? platfromDimension.hashCode() : 0;
        result = 31 * result + (dateDimension != null ? dateDimension.hashCode() : 0);
        result = 31 * result + (kpiDimension != null ? kpiDimension.hashCode() : 0);
        return result;
    }

    public PlatformDimension getPlatformDimension() {
        return platfromDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platfromDimension= platformDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}


