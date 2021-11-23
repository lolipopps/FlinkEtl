package com.flink.register;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class RegisterTableSinkTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(2);

        String[] fieldNames = {"name", "age", "city"};
        TypeInformation[] fieldTypes = {Types.STRING(), Types.INT(), Types.STRING()};

        TableSource csvSource = new CsvTableSource("///home/intsmaze/flink/table/data", fieldNames, fieldTypes);

        tableEnv.registerTableSource("Person", csvSource);

        TableSink csvSink = new CsvTableSink("///home/intsmaze/flink/table/sink/", "|");

        String[] outputFieldNames = {"name", "city"};
        TypeInformation[] outputFieldTypes = {Types.STRING(), Types.STRING()};

        tableEnv.registerTableSink("CsvSinkTable", outputFieldNames, outputFieldTypes, csvSink);

        Table result = tableEnv.sqlQuery("SELECT name,city FROM Person WHERE age>30");
        result.insertInto("CsvSinkTable");

        tableEnv.sqlUpdate("INSERT INTO CsvSinkTable SELECT name,city FROM Person WHERE age>30");

        env.execute();
    }

}
