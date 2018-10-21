package com.qianfeng.util;

import com.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;


//查看会员id,是否是新增用户,建议过滤不合法的会员id
public class MemberUtil {
    private static Map<String,Boolean> cache =new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 1000;
        }
    };

    //检测会员id是否合法,true为合法.false为不合法
    public static  boolean checkMemberId(String memberid){
        String regex ="^[0-9a-zA-Z].*$";
        if (StringUtils.isEmpty(memberid)){
            return memberid.trim().matches(regex);
        }
        return false;
    }
    /**
     * 是否是一个新增的会员
     * @param memberId
     * @param conn
     * @param conf
     * @return  true是新增会员，false不是新增会员
     */
    public static boolean isNewMember(String memberId, Connection conn, Configuration conf){

        PreparedStatement ps = null;
        ResultSet rs = null;
        Boolean res = false;
        try {
            res = cache.get(memberId);
            if(res == null){
                String sql = conf.get(GlobalConstants.PREFIX_TOTAL+"member_info");
                ps = conn.prepareStatement(sql);
                ps.setString(1,memberId);
                rs = ps.executeQuery();
                if(rs.next()){
                    res = false;
                } else {
                    res = true;
                }
                //添加到cache中
                cache.put(memberId,res);

            }
        } catch (SQLException e){
            e.printStackTrace();
        }
        return res == null ? false : res.booleanValue();
    }
}
