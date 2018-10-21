package com.qianfeng.analystic.mr.nm;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import com.qianfeng.analystic.mr.IOutputWritter;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiType;
import com.qianfeng.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/7/30 11:07
 * @Description: 新增跃会员的ps赋值
 */
public class NewMemberOuputWrtter implements IOutputWritter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue v = (MapWritableValue) value;

        //为ps赋值
        int i = 0;
        switch (v.getKpi()){
            case BROWSER_NEW_MEMBER:
            case NEW_MEMBER:
                int newUsers = ((IntWritable) ((MapWritableValue)value).
                        getValue().get(new IntWritable(-1))).get();
                ps.setInt(++i,convert.getDimensionIdByDimension(statsUserDimension.getStatsCommonDimension().getDateDimension()));
                ps.setInt(++i,convert.getDimensionIdByDimension(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
                if(v.getKpi().equals(KpiType.BROWSER_NEW_MEMBER)){
                    ps.setInt(++i,convert.getDimensionIdByDimension(statsUserDimension.getBrowserDimension()));
                }
                ps.setInt(++i,newUsers);
                ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(++i,newUsers);
                break;
            case MEMBER_INFO:
            String memberId = ((Text) ((MapWritableValue)value).
                    getValue().get(new IntWritable(-1))).toString();
            ps.setString(++i,memberId);
            ps.setDate(++i,new Date(TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE))));
            ps.setLong(++i,TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE)));
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setDate(++i,new Date(TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE))));
            break;
            default:
                throw new RuntimeException("对不起，不支持结果写出.");
        }
        //添加到批处理中
        ps.addBatch();
    }
}
