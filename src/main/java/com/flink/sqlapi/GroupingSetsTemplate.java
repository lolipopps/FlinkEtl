package com.flink.sqlapi;
import com.flink.bean.ClickBean;
import com.flink.sql.PrepareData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;


public class GroupingSetsTemplate {



    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        DataSet<ClickBean> streamSource = env.fromCollection(PrepareData.getClicksData());
        tableEnv.registerDataSet("Clicks", streamSource, "user,url,time");


        Table table = tableEnv.sqlQuery("SELECT user,url,count(1) FROM Clicks GROUP BY GROUPING SETS ((user), (url))");

        DataSet<Row> result = tableEnv.toDataSet(table, Row.class);
        result.print();
    }
}
