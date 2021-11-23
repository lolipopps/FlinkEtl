package com.flink.register;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;


public class RegisterTableTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String[] fieldNames = {"name", "age", "city"};
        TypeInformation[] fieldTypes = {Types.STRING(), Types.INT(), Types.STRING()};

        TableSource csvSource = new CsvTableSource("///home/intsmaze/flink/table/data", fieldNames, fieldTypes);

        tableEnv.registerTableSource("Person", csvSource);

        Table result = tableEnv.sqlQuery("SELECT name,age,city FROM Person WHERE age>30");

        tableEnv.registerTable("resultTable", result);

        Table resultTwo = tableEnv.sqlQuery("SELECT name,city FROM resultTable");

        tableEnv.toAppendStream(resultTwo, Row.class).print();

        env.execute();
    }


}
