package com.qianfeng.analystic.model.dim.base;

//浏览器维度类
import com.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class BrowserDimension extends BaseDimension{
    private int id;
    private String browserName;
    private String browserVersion;

    public BrowserDimension(){

    }

    public BrowserDimension(String browserName, String browserVersion) {
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    /**
     * 获取当前类的实例
     * @param browserName
     * @param browserVersion
     * @return
     */
    public static BrowserDimension newInstance(String browserName, String browserVersion){
        BrowserDimension browserDimension = new BrowserDimension();
        browserDimension.browserName = browserName;
        browserDimension.browserVersion = browserVersion;
        return browserDimension;
    }

    /**
     *构建浏览器维度的集合对象
     * @param browserName
     * @param browserVersion
     * @return
     */
    public static List<BrowserDimension> buildList(String browserName, String browserVersion){
        List<BrowserDimension> li = new ArrayList<BrowserDimension>();
        if(StringUtils.isEmpty(browserName)){
            browserName = browserVersion = GlobalConstants.DEFAULT_VALUE;
        }

        if(StringUtils.isEmpty(browserVersion)){
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }

        li.add(newInstance(browserName,browserVersion));
        li.add(newInstance(browserName,GlobalConstants.ALL_OF_VALUE));
        return li;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.browserName);
        out.writeUTF(this.browserVersion);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.browserName = in.readUTF();
        this.browserVersion = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o == this){
            return 0;
        }
        BrowserDimension other = (BrowserDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return tmp;
        }
        tmp = this.browserName.compareTo(other.browserName);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.browserVersion.compareTo(other.browserVersion);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BrowserDimension that = (BrowserDimension) o;

        if (id != that.id) return false;
        if (browserName != null ? !browserName.equals(that.browserName) : that.browserName != null) return false;
        return browserVersion != null ? browserVersion.equals(that.browserVersion) : that.browserVersion == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (browserName != null ? browserName.hashCode() : 0);
        result = 31 * result + (browserVersion != null ? browserVersion.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }
}
