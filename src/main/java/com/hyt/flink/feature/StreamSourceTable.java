package com.hyt.flink.feature;

import com.hyt.flink.util.ExecutionEnvUtil;
import lombok.Data;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.*;

@Data
public class StreamSourceTable {
    StreamTableEnvironment tableEnv;
    HashMap<String, String> tables = new HashMap<>();

    public StreamSourceTable(StreamTableEnvironment env) {
        this.tableEnv = env;
        init();
        registView();
    }

    private void init() {
        Map<String, String> paras = ExecutionEnvUtil.PARAMETER_TOOL.toMap();
        for (Map.Entry<String, String> para : paras.entrySet()) {
            if (para.getKey().startsWith("kafkaTable")) {
                String view = para.getKey().split("\\.")[1];
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("create view `" + view + "` as select rowTime \n,");
                String[] columns = para.getValue().split(",");
                for (String column : columns) {
                    stringBuilder.append("getJsonObject(content,'" + column + "') as `" + column + "` \n,");
                }
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                stringBuilder.append("from all_table where logType='" + view + "'");
                tables.put(view, stringBuilder.toString());
            }
        }
    }

    public ArrayList<String> getTabelNames() {
        ArrayList<String> res = new ArrayList<>();
        for (Map.Entry<String, String> table : tables.entrySet()) {
            res.add(table.getKey());
        }
        Collections.sort(res);
        return res;
    }

    public void registView() {
        for (Map.Entry<String, String> tableView : tables.entrySet()) {
            tableEnv.executeSql(tableView.getValue());
        }

    }

    public void printRegistTable() {
        String[] res = tableEnv.listViews();
        for (String re : res) {
            System.out.println(re);
        }
    }

    public void printTableSchemel(String table) {
        System.out.println(tables.get(table));
    }


}
