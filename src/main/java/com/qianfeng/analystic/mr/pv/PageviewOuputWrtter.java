package com.qianfeng.analystic.mr.pv;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import com.qianfeng.analystic.mr.IOutputWritter;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/7/30 11:07
 * @Description: pv的ps赋值
 */
public class PageviewOuputWrtter implements IOutputWritter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue v = (MapWritableValue) value;
        int newUsers = ((IntWritable) ((MapWritableValue)value).
                getValue().get(new IntWritable(-1))).get();

        //为ps赋值
        int i = 0;
        ps.setInt(++i,convert.getDimensionIdByDimension(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i,convert.getDimensionIdByDimension(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
        ps.setInt(++i,convert.getDimensionIdByDimension(statsUserDimension.getBrowserDimension()));
        ps.setInt(++i,newUsers);
        ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i,newUsers);

        //添加到批处理中
        ps.addBatch();
    }
}
