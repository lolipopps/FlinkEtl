package com.flink.connector.sink;

import com.flink.sql.PrepareData;
import com.flink.bean.Person;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

import java.util.List;


public class CsvSinkTemplate {



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        List<Person> clicksData = PrepareData.getPersonData();
        DataStream<Person> dataStream = env.fromCollection(clicksData);

        tableEnv.registerDataStream("Person", dataStream, "name,age,city");

        CsvTableSink sink = new CsvTableSink(
                "///home/intsmaze/flink/table/csv",
                "|",
                1,
                FileSystem.WriteMode.OVERWRITE);

        tableEnv.registerTableSink(
                "csvOutputTable",
                new String[]{"name", "age", "city"},
                new TypeInformation[]{Types.STRING, Types.LONG, Types.STRING},
                sink);

        tableEnv.sqlUpdate("INSERT INTO csvOutputTable SELECT name,age,city FROM Person WHERE age <20 ");

        env.execute();
    }
}
