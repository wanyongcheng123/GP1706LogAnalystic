package com.qianfeng.common;
//时间枚举
public enum DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour")
    ;

    public String type;

    DateEnum(String type) {
        this.type = type;
    }

    /**
     * 根据type获取时间枚举
     * @param type
     * @return
     */
    public DateEnum valueOfType(String type){
        for (DateEnum date : values()){
            if(type.equals(date.type)){
                return date;
            }
        }
        throw  new RuntimeException("该type暂不支持获取时间枚举."+type);
    }
}

