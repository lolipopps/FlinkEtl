package com.flink.feature;

import com.flink.config.PropertiesConstants;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamTableFeature {
    HashMap<String, String> sqls = new HashMap<>();
    StreamTableEnvironment tableEnv;

    public StreamTableFeature(StreamTableEnvironment env) {
        this.tableEnv = env;
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void init() throws IOException {
//        ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SQL_FILE);
        InputStream inputStream = ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.SQL_FILE);
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        Pattern pattern = Pattern.compile("CREATE VIEW(.*)AS");
        String[] allSql = result.toString("UTF-8").split(";");
        for (String sql : allSql) {
            Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                sqls.put(matcher.group(1).trim(), sql);
                tableEnv.executeSql(sql);
            }
        }
    }


}
