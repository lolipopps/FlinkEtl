package com.flink.feature;

import com.flink.config.PropertiesConstants;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class StreamSinkTable {
    HashMap<String, String> sqls = new HashMap<>();
    StreamTableEnvironment tableEnv;

    public StreamSinkTable(StreamTableEnvironment env) {
        this.tableEnv = env;
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void init() throws IOException {
        InputStream inputStream = ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.SINK_SQL_FILE);
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        String[] allSql = result.toString("UTF-8").split(";");
        for (String excSql : allSql) {
            System.out.println(excSql);
            tableEnv.executeSql(excSql);
            sqls.put(excSql.substring(0, 10), excSql);
        }

    }


    public void printRegistTable() {
        String[] res = tableEnv.listTables();
        for (String re : res) {
            System.out.println(re);
        }
    }

}
