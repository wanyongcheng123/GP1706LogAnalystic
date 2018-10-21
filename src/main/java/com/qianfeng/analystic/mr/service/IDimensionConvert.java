package com.qianfeng.analystic.mr.service;

import com.qianfeng.analystic.model.dim.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 16:14
 * @Description:根据维度对象获取对应维度的id的接口
 */
public interface IDimensionConvert {
    /**
     * 根据维度对象获取对应维度的id的接口
     * @param dimension
     * @return
     * @throws IOException
     * @throws SQLException
     */
    int getDimensionIdByDimension(BaseDimension dimension) throws IOException,SQLException;
}
