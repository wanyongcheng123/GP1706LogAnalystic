package com.qianfeng.analystic.model.dim.value;

import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 14:55
 * @Description:map或者是reduce阶段输出value的类型的顶级父类
 */
public abstract class OutputValueBaseWritable implements Writable{

    /**
     * 获取kpi
     * @return
     */
    public abstract KpiType getKpi(); //获取一个kpi的抽象方法
}
