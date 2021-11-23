package com.flink.udf.table;


import com.flink.bean.OrderBean;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;


public class TableFunctionTemplate {



    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        DataSet<OrderBean> input = env.fromElements(
                new OrderBean(1L, "beer#intsmaze", 3),
                new OrderBean(1L, "flink#intsmaze", 4),
                new OrderBean(3L, "rubber#intsmaze", 2));

        tableEnv.registerDataSet("orderTable", input, "user,product,amount");

//        tableEnv.registerFunction("splitFunction", new SplitTable("#"));

        tableEnv.registerFunction("splitFunction", new SplitTableTypeInfor("#"));

        Table sqlCrossResult = tableEnv.sqlQuery("SELECT user,product,amount,word, length FROM orderTable, " +
                "LATERAL TABLE(splitFunction(product)) AS T(word, length)");

        Table sqlLeftResult = tableEnv.sqlQuery("SELECT user,product,amount,word, length FROM orderTable LEFT JOIN LATERAL TABLE(splitFunction(product)) AS T(word, length) ON TRUE");

        tableEnv.toDataSet(sqlCrossResult, Row.class).print("CROSS JOIN");
        tableEnv.toDataSet(sqlLeftResult, Row.class).print("LEFT JOIN ");

        env.execute("TableFunctionTemplate");
    }
}
