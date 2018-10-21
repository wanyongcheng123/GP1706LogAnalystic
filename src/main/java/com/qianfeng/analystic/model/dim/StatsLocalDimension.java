package com.qianfeng.analystic.model.dim;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;
import com.qianfeng.analystic.model.dim.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 14:42
 * @Description:封装用地域模块中map和reduce阶段输出的key的类型
 */
public class StatsLocalDimension extends StatsBaseDimension {

    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDimension locationDimension = new LocationDimension();

    public StatsLocalDimension() {

    }

    public StatsLocalDimension(StatsCommonDimension statsCommonDimension, LocationDimension locationDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.locationDimension = locationDimension;
    }

    /**
     * 克隆当前对象一个实例
     *
     * @param dimension
     * @return
     */
    public static StatsLocalDimension clone(StatsLocalDimension dimension) {
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
        LocationDimension locationDimension = new LocationDimension(dimension.locationDimension.getId(),
                dimension.locationDimension.getCountry(),
                dimension.locationDimension.getProvince(), dimension.locationDimension.getCity());
        return new StatsLocalDimension(statsCommonDimension, locationDimension);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.locationDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.locationDimension.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        StatsLocalDimension other = (StatsLocalDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (tmp != 0) {
            return tmp;
        }

        tmp = this.locationDimension.compareTo(other.locationDimension);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsLocalDimension that = (StatsLocalDimension) o;

        if (statsCommonDimension != null ? !statsCommonDimension.equals(that.statsCommonDimension) : that.statsCommonDimension != null)
            return false;
        return locationDimension != null ? locationDimension.equals(that.locationDimension) : that.locationDimension == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommonDimension != null ? statsCommonDimension.hashCode() : 0;
        result = 31 * result + (locationDimension != null ? locationDimension.hashCode() : 0);
        return result;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }
}
