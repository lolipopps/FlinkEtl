package com.hyt.flink.feature;

import com.hyt.flink.config.PropertiesConstants;
import com.hyt.flink.util.ExecutionEnvUtil;
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
            init(null);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public StreamTableFeature(StreamTableEnvironment env,String path) {
        this.tableEnv = env;
        try {
            init(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void init(String path) throws IOException {
        InputStream inputStream = null;
        if(null != path){
            inputStream = ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(path);
        }else {
            inputStream = ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.FEATURE_SQL_FILE);
        }
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

    public void printRegistTable() {
        String[] res = tableEnv.listTables();
        for (String re : res) {
            System.out.println(re);
        }
    }

}
