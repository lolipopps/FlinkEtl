package com.hyt.flink.util;

import java.sql.*;
/**
 * @Author: hyt
 * @License: (C) Copyright 2020-2020, xxx Corporation Limited.
 * @Contact: xxx@xxx.com
 * @Version: 1.0
 * @Description:
 */
public class HiveJdbcUtils {
    //netstat -tunlp
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String connctUrl = "jdbc:hive2://hadoop:10000/flink";
    private static String userName = "root";
    private static String password = "";
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(connctUrl, userName, password);
        Statement stmt = con.createStatement();
        String tableName = "test";
        //创建表
        stmt.execute("create table  if not exists " + tableName + " (id bigint, name string,age int)");
        //查询
        ResultSet res = stmt.executeQuery( "select * from " + tableName );
        while (res.next()) {
            //      System.out.println( res.getLong(1) +"," + res.getString(2) + "," + res.getInt(3)  );
        }
        stmt.close();
        con.close();
    }
}
