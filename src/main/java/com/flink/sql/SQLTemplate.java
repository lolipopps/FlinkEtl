package com.flink.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;


public class SQLTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String[] fieldNames = {"name", "age", "city"};

        TypeInformation[] fieldTypes = {Types.STRING(), Types.INT(), Types.STRING()};
        TableSource csvSource = new CsvTableSource("///home/intsmaze/flink/table/data", fieldNames, fieldTypes);

        tableEnv.registerTableSource("Person", csvSource);

        Table table = tableEnv
                .sqlQuery("SELECT name,count(1) FROM Person WHERE age >30 GROUP BY name");

        tableEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}