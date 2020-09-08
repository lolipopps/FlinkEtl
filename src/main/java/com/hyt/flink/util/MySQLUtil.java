package com.hyt.flink.util;

import com.hyt.flink.config.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.*;

@Slf4j
public class MySQLUtil {
    public static Connection getConnection() {
        ParameterTool prop = ExecutionEnvUtil.PARAMETER_TOOL;
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = prop.get(PropertiesConstants.MYSQL_URL);
            String user = prop.get(PropertiesConstants.MYSQL_USERNAME);
            String password = prop.get(PropertiesConstants.MYSQL_PASSWORD);
            //注意，这里替换成你自己的mysql 数据库路径和用户名、密码
            con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            //      System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }


}
