package com.qianfeng.analystic.hive;

import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.analystic.mr.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.util.TimeUtil;
import org.apache.commons.lang.StringUtils;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/8/2 15:27
 * @Description:获取时间维度id的udf
 */
public class DateDimensionUdf extends UDF {

    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(Text dt){
        String d = dt.toString();
        if(StringUtils.isEmpty(d)){
            d = TimeUtil.getYesterdayDate();
        }
        DateDimension dd = DateDimension.buildDate(TimeUtil.parserString2Long(d), DateEnum.DAY);
        int id = -1;
        try {
            id = convert.getDimensionIdByDimension(dd);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }


    public int evaluate(Text dt,int i){
        String d = dt.toString();
        if(StringUtils.isEmpty(d)){
            d = TimeUtil.getYesterdayDate();
        }
        DateDimension dd = DateDimension.buildDate(TimeUtil.parserString2Long(d), DateEnum.DAY);
        int id = -1;
        try {
            id = convert.getDimensionIdByDimension(dd);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id+i;
    }
    public static void main(String[] args) {
        System.out.println(new DateDimensionUdf().evaluate(new Text("2018-07-26")));
        System.out.println(new DateDimensionUdf().evaluate(new Text("2018-07-26"),2));
    }

}
