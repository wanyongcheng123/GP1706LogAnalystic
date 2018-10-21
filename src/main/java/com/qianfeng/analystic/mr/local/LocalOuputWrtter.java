package com.qianfeng.analystic.mr.local;

import com.qianfeng.analystic.model.dim.StatsLocalDimension;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;

import com.qianfeng.analystic.model.dim.value.reduce.LocalOutputValue;
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
 * @Description: localps赋值
 */
public class LocalOuputWrtter implements IOutputWritter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {
        StatsLocalDimension statsLocalDimension = (StatsLocalDimension) key;
        LocalOutputValue v = (LocalOutputValue) value;


        //为ps赋值
        int i = 0;
        ps.setInt(++i,convert.getDimensionIdByDimension(statsLocalDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i,convert.getDimensionIdByDimension(statsLocalDimension.getStatsCommonDimension().getPlatformDimension()));
        ps.setInt(++i,convert.getDimensionIdByDimension(statsLocalDimension.getLocationDimension()));
        ps.setInt(++i,v.getAus());
        ps.setInt(++i,v.getSessions());
        ps.setInt(++i,v.getBoundSessions());
        ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i,v.getAus());
        ps.setInt(++i,v.getSessions());
        ps.setInt(++i,v.getBoundSessions());

        //添加到批处理中
        ps.addBatch();
    }
}
