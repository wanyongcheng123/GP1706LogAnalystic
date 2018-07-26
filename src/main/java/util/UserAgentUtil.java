package util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

public class UserAgentUtil {
private static final Logger logger =Logger.getLogger(UserAgentInfo.class);
    UserAgentInfo info =new UserAgentInfo();
    //获取uasparser对象
    public static UASparser uaSparser =null;
    static {
        try {
            uaSparser =new UASparser(OnlineUpdater.getVendoredInputStream());
        }catch (IOException e){

        }
    }
    //解析浏览器的代理对象
    public UserAgentInfo parserUserAgent(String userAgent){
        if (StringUtils.isEmpty(userAgent)){
            return null;
        }
        //使用uasparser获取代理对象
        try {
            cz.mallat.uasparser.UserAgentInfo ua =uaSparser.parse(userAgent);
            if(ua !=null){
                //为info设置信息
                info.setBrowserName(ua.getUaFamily());
                info.setBrowserVersion(ua.getBrowserVersionInfo());
                info.setOnName(ua.getOsFamily());
                info.setOsVersion(ua.getOsName());
            }
        }catch (IOException e){
            logger.warn("useragent解析异常.",e);
        }
        return info;
    }
    //封装浏览器相关熟悉
    public static class UserAgentInfo {
        private String browserName;
        private String browserVersion;
        private String onName;

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", onName='" + onName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }

        private String osVersion;

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

        public String getOnName() {
            return onName;
        }

        public void setOnName(String onName) {
            this.onName = onName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

    }
}
