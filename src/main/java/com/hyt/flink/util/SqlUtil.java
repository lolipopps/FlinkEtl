package com.hyt.flink.util;

import com.hyt.flink.config.PropertiesConstants;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: hyt
 * @License: (C) Copyright 2020-2020, xxx Corporation Limited.
 * @Contact: xxx@xxx.com
 * @Version: 1.0
 * @Description:
 */
public class SqlUtil {

    public static void execuPathSql(StreamTableEnvironment tableEnv, String path) {
        InputStream inputStream = null;
        if (null != path) {
            inputStream = ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(path);
        } else {
            inputStream = ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.FEATURE_SQL_FILE);
        }
        ByteArrayOutputStream sql = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length = 0;
        while (true) {
            try {
                if (!((length = inputStream.read(buffer)) != -1)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            sql.write(buffer, 0, length);
        }
        tableEnv.executeSql(sql.toString());

    }

}
