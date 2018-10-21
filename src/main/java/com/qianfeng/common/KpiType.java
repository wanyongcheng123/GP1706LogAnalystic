package com.qianfeng.common;

/**
 * @Auther:
 * @Date: 2018/7/27 14:57
 * @Description:kpi的枚举
 */
public enum KpiType {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    SESSION("session"),
    BROWSER_SESSION("browser_session"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    HOURLY_SESSION("hourly_session"),
    PAGEVIEW("pageview"),
    LOCAL("local"),
    ;
    public String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 根据kpi的name获取kpi的枚举
     * @param kpiName
     * @return
     */
    public static KpiType valueOfType(String kpiName){
        for (KpiType kpi : values()){
            if(kpiName.equals(kpi.kpiName)){
                return kpi;
            }
        }
        throw  new RuntimeException("该kpiName暂不支持获取kpi的枚举."+kpiName);
    }
}
