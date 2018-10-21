package com.qianfeng.analystic.hive;

import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.analystic.mr.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/8/2 15:27
 * @Description:获取平台维度id的udf
 */
public class PlatformDimensionUdf extends UDF {

    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String platform){
        if(StringUtils.isEmpty(platform)){
            platform = GlobalConstants.DEFAULT_VALUE;
        }
        PlatformDimension pl = new PlatformDimension(platform);
        int id = -1;
        try {
            id = convert.getDimensionIdByDimension(pl);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new PlatformDimensionUdf().evaluate(("website")));
    }

}
