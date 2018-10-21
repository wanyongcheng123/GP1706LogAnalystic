package com.qianfeng.util;
//获取mysql的连接和关闭
import com.qianfeng.common.GlobalConstants;

import java.sql.*;

/**
 * @Auther: wyc
 * @Date: 2018/7/27 17:09
 * @Description:
 */
public class JdbcUtil {
    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
//获取mysql的连接
    public static Connection getConn(){
        return null;
    }
    Connection conn =null ;
   // conn =DriverManager.getConection(GlobalConstants.URL);

//关闭相对对象
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs){
if(conn !=null){
    try {
        conn.close();
    } catch (SQLException e) {
        e.printStackTrace();
    }
}
if (ps !=null){
    try {
        ps.close();
    } catch (SQLException e) {
        e.printStackTrace();
    }
}
if (rs!=null){
    try {
        rs.close();
    } catch (SQLException e) {
        e.printStackTrace();
    }
}
    }
}
