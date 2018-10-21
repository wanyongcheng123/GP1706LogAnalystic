package com.qianfeng.analystic.mr.service.impl;

import com.qianfeng.analystic.model.dim.base.*;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.util.JdbcUtil;
import jdk.nashorn.internal.scripts.JD;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 16:18
 * @Description:操作维度表的接口实现
 */
public class IDimensionConvertImpl implements IDimensionConvert{
    private static final Logger logger = Logger.getLogger(IDimensionConvertImpl.class);
    // 维度：维度对应的id   缓存
    private Map<String,Integer> cache = new LinkedHashMap<String,Integer>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
           return this.size() > 1000;
        }
    };


    /**
     * 获取维度id
     * 0、先查询缓存中是否存在对应维度，如果直接取出返回
     * 1、先用根据维度属性去查询数据库，如果有就返回维度对应的id
     * 2、如果没有，则先插入再返回
     * plaformDimension("website")
     * plaformDimension("website")
     * @param dimension
     * @return
     * @throws IOException
     * @throws SQLException
     */
    @Override
    public int getDimensionIdByDimension(BaseDimension dimension) throws IOException, SQLException {
        String cacheKey = this.buildCache(dimension);
        if(this.cache.containsKey(cacheKey)){
            return this.cache.get(cacheKey);
        }

        //代码走到这儿，缓存中没有，去查询数据库
        String [] sqls = null;
        if(dimension instanceof DateDimension){
            sqls = this.buildDateSqls();
        } else if(dimension instanceof PlatformDimension){
            sqls = this.buildPlatformSqls();
        } else if(dimension instanceof BrowserDimension){
            sqls = this.buildBrowserSqls();
        } else if(dimension instanceof KpiDimension){
            sqls = this.buildKpiSqls();
        } else if(dimension instanceof LocationDimension){
            sqls = this.buildLocalSqls();
        } else if(dimension instanceof EventDimension){
            sqls = this.buildEventSqls();
        }

        Connection conn = JdbcUtil.getConn();
        int id = -1;
        synchronized (this) {
           id = this.execute(sqls,dimension,conn);
        }
        //将获取到的id添加到缓存
        this.cache.put(cacheKey,id);
        return id;
    }




    /**
     * 构建sqls
     * @return
     */
    private String[] buildPlatformSqls() {
        String select = "select `id` from `dimension_platform` where `platform_name` = ?";
        String insert = "insert into `dimension_platform`(`platform_name`) values(?)";
        return new String[]{select,insert};
    }

    private String[] buildDateSqls() {
        String select = "select `id` from `dimension_date` where `year`  = ? and `season` = ? and `month` = ? and `week` = ? and `day` = ? and `calendar` = ? and `type` = ?";
        String insert = "insert into `dimension_date`(`year`,`season`, `month`,`week`,`day`,`calendar`,`type`) values(?,?,?,?,?,?,?)";
        return new String[]{select,insert};
    }

    private String[] buildBrowserSqls() {
        String select = "select `id` from `dimension_browser` where  `browser_name` = ? and `browser_version` = ?";
        String insert = "insert into `dimension_browser`(`browser_name`,`browser_version`) values(?,?)";
        return new String[]{select,insert};
    }

    private String[] buildKpiSqls() {
        String select = "select `id` from `dimension_kpi` where `kpi_name` = ?";
        String insert = "insert into `dimension_kpi`(`kpi_name`) values(?)";
        return new String[]{select,insert};
    }

    private String[] buildLocalSqls() {
        String select = "select `id` from `dimension_location` where `country` = ? and `province` = ? and `city` = ?";
        String insert = "insert into `dimension_location`(`country` ,`province`,`city`) values(?,?,?)";
        return new String[]{select,insert};
    }

    private String[] buildEventSqls() {
        String select = "select `id` from `dimension_event` where `category` = ? and `action` = ?";
        String insert = "insert into `dimension_event`(`category` ,`action`) values(?,?)";
        return new String[]{select,insert};
    }

    private String buildCache(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();
        if(dimension instanceof DateDimension){
            sb.append("date_");
            DateDimension date = (DateDimension) dimension;
            sb.append(date.getYear()).append(date.getSeason())
                    .append(date.getMonth()).append(date.getWeek())
                    .append(date.getDay()).append(date.getType());
        } else if(dimension instanceof PlatformDimension){
            sb.append("platform_");
            PlatformDimension platform = (PlatformDimension) dimension;
            sb.append(platform.getPlatformName());
        } else if(dimension instanceof BrowserDimension){
            sb.append("browser_");
            BrowserDimension browser = (BrowserDimension) dimension;
            sb.append(browser.getBrowserName());
            sb.append(browser.getBrowserVersion());
        } else if(dimension instanceof KpiDimension){
            sb.append("kpi_");
            KpiDimension kpi = (KpiDimension) dimension;
            sb.append(kpi.getKpiName());

        } else if(dimension instanceof LocationDimension){
            sb.append("local_");
            LocationDimension local = (LocationDimension) dimension;
            sb.append(local.getCountry());
            sb.append(local.getProvince());
            sb.append(local.getCity());
        } else if(dimension instanceof EventDimension){
            sb.append("event_");
            EventDimension event = (EventDimension) dimension;
            sb.append(event.getCategory());
            sb.append(event.getAction());
        }
        return sb.length() == 0 ? null : sb.toString();
    }


    /**
     * 执行
     * @param sqls
     * @param dimension
     * @param conn
     * @return
     */
    private int execute(String[] sqls, BaseDimension dimension, Connection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try{
            //先查询
            ps = conn.prepareStatement(sqls[0]);
            this.setArgs(dimension,ps);
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getInt(1);
            }
            //查询不到，然后插入再取值
            ps = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            this.setArgs(dimension,ps);
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if(rs.next()){
                return rs.getInt(1);
            }

        } catch (SQLException e){
            logger.warn("执行维度sql异常.",e);
        } finally {
            JdbcUtil.close(conn,ps,rs);
        }
        throw new RuntimeException("查询和插入sql都异常.");
    }

    /**
     * 设置参数
     * @param dimension
     * @param ps
     */
    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try {
            int i = 0;
            if(dimension instanceof DateDimension){
                DateDimension date = (DateDimension) dimension;
                ps.setInt(++i,date.getYear());
                ps.setInt(++i,date.getSeason());
                ps.setInt(++i,date.getMonth());
                ps.setInt(++i,date.getWeek());
                ps.setInt(++i,date.getDay());
                ps.setDate(++i,new Date(date.getCalendar().getTime()));
                ps.setString(++i,date.getType());
            } else if(dimension instanceof PlatformDimension){
                PlatformDimension platform = (PlatformDimension) dimension;
                ps.setString(++i,platform.getPlatformName());
            } else if(dimension instanceof BrowserDimension){
                BrowserDimension browser = (BrowserDimension) dimension;
                ps.setString(++i,browser.getBrowserName());
                ps.setString(++i,browser.getBrowserVersion());
            } else if(dimension instanceof KpiDimension){
                KpiDimension kpi = (KpiDimension) dimension;
                ps.setString(++i,kpi.getKpiName());
            } else if(dimension instanceof LocationDimension){
                LocationDimension local = (LocationDimension) dimension;
                ps.setString(++i,local.getCountry());
                ps.setString(++i,local.getProvince());
                ps.setString(++i,local.getCity());
            } else if(dimension instanceof EventDimension){
                EventDimension event = (EventDimension) dimension;
                ps.setString(++i,event.getCategory());
                ps.setString(++i,event.getAction());
            }
        } catch (SQLException e) {
            logger.warn("设置参数异常.",e);
        }
    }
}
