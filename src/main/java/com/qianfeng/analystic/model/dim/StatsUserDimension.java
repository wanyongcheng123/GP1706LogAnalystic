package com.qianfeng.analystic.model.dim;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther:
 * @Date: 2018/7/27 14:42
 * @Description:封装用户模块和浏览器模块中map和reduce阶段输出的key的类型
 */
public class StatsUserDimension extends StatsBaseDimension {

    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private BrowserDimension browserDimension = new BrowserDimension();

    public StatsUserDimension(){

    }

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDimension browserDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.browserDimension = browserDimension;
    }

    /**
     * 克隆当前对象一个实例
     * @param dimension
     * @return
     */
    public static StatsUserDimension clone(StatsUserDimension dimension){
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
        BrowserDimension browserDimension = new BrowserDimension(
        );
        return new StatsUserDimension(statsCommonDimension,browserDimension);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.browserDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.browserDimension.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o == this){
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if(tmp != 0){
            return tmp;
        }

        tmp = this.browserDimension.compareTo(other.browserDimension);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsUserDimension that = (StatsUserDimension) o;

        if (statsCommonDimension != null ? !statsCommonDimension.equals(that.statsCommonDimension) : that.statsCommonDimension != null)
            return false;
        return browserDimension != null ? browserDimension.equals(that.browserDimension) : that.browserDimension == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommonDimension != null ? statsCommonDimension.hashCode() : 0;
        result = 31 * result + (browserDimension != null ? browserDimension.hashCode() : 0);
        return result;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }
}
