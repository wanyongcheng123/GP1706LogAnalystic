package com.qianfeng.util;


import com.qianfeng.common.DateEnum;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Auther: lyd
 * @Date: 2018/7/26 14:25
 * @Description:操作时间工具方法
 */
public class TimeUtil {
    private static final Logger logger = Logger.getLogger(TimeUtil.class);
    private static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    /**
     * 验证日期是否合法
     * @param date
     * @return
     */
    public static boolean isValidateDate(String date){
        Matcher matcher = null;
        boolean res = false;
        String regexp = "^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}";
        if(date != null){
            Pattern pattern = Pattern.compile(regexp);
            matcher = pattern.matcher(date);
        }
        if(matcher != null){
            res = matcher.matches();
        }
        return res;
    }

    /**
     * 获取昨天的日期,不传格式为默认的
     * @return
     */
    public static String getYesterdayDate(){
        return getYesterdayDate(DEFAULT_DATE_FORMAT);
    }

    /**
     * 获取昨天的日期
     * @return
     */
    public static String getYesterdayDate(String dateformat){
        SimpleDateFormat sdf = new SimpleDateFormat(dateformat);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        return sdf.format(calendar.getTime());
    }


    /**
     * 将字符串的日期转换成时间戳, 默认的格式
     * @param date
     * @return
     */
    public static long parserString2Long(String date){
        return parserString2Long(date,DEFAULT_DATE_FORMAT);
    }

    /**
     * 将字符串的日期转换成时间戳 2018-07-26  15127398848923
     * @param date
     * @return
     */
    public static long parserString2Long(String date,String parttern){
        SimpleDateFormat sdf = new SimpleDateFormat(parttern);
        Date dt = null;
        try {
            dt = sdf.parse(date);

        } catch (ParseException e) {
            logger.warn("解析字符串的date为时间戳异常",e);
        }
        return dt == null ? 0 : dt.getTime();
    }


    /**
     * 将时间戳转换成日期, 默认的格式
     * @param time
     * @return
     */
    public static String parserLong2String(long time){
        return parserLong2String(time,DEFAULT_DATE_FORMAT);
    }

    /**
     * 将时间戳转换为指定格式的日期
     * @param time
     * @param format
     * @return
     */
    public static String parserLong2String(long time,String format){
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return new SimpleDateFormat(format).format(calendar.getTime());
    }
    /***
     * 根据时间戳获取时间相关信息
     * @param time
     * @param type
     * @return
     */
    public static int getDateInfo(long time, DateEnum type) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if(type.equals(DateEnum.YEAR)){
            return calendar.get(Calendar.YEAR);
        }
        if(type.equals(DateEnum.SEASON)){
            int month = calendar.get(Calendar.MONTH) + 1;
            return (month +2) / 3;
        }
        if(type.equals(DateEnum.MONTH)){
            return calendar.get(Calendar.MONTH) + 1;
        }
        if(type.equals(DateEnum.WEEK)){
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }

        if(type.equals(DateEnum.DAY)){
            return calendar.get(Calendar.DAY_OF_MONTH);
        }
        if(type.equals(DateEnum.HOUR)){
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
        throw  new RuntimeException("该type不支持获取对应值."+type.type);
    }

    /**
     * 根据时间戳获取时间戳所在周第一天的时间戳
     * @param time
     * @return
     */
    public static long getFirstDayOfWeek(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.DAY_OF_WEEK,1);
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        return calendar.getTimeInMillis();
    }


    public static void main(String[] args) {
//        System.out.println(isValidateDate("2018-7-26"));
//        System.out.println(getYesterdayDate("yyyy/MM/dd"));
//        System.out.println(parserString2Long("2018-07-26"));
//        System.out.println(parserLong2String(1532534400000L));
        System.out.println(getDateInfo(1532534400000L, DateEnum.YEAR));
        System.out.println(getDateInfo(1532534400000L,DateEnum.SEASON));
        System.out.println(getDateInfo(1532534400000L,DateEnum.DAY));
        System.out.println(getFirstDayOfWeek(1532534400000L));
    }



}

