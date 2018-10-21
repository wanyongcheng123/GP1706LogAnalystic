package com.qianfeng.analystic.hive;


import com.qianfeng.analystic.model.dim.base.EventDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.analystic.mr.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/8/2 15:27
 * @Description:获取event维度id的udf
 */
public class EventDimensionUdf extends UDF {

    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String category,String action){
        if(StringUtils.isEmpty(category)){
            category = action = GlobalConstants.DEFAULT_VALUE;
        }

        if(StringUtils.isEmpty(action)){
            action = GlobalConstants.DEFAULT_VALUE;
        }
        EventDimension event = new EventDimension(category,action);
        int id = -1;
        try {
            id = convert.getDimensionIdByDimension(event);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new EventDimensionUdf().evaluate("aa","cc"));
    }

}
