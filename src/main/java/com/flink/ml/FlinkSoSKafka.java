package com.flink.ml;


import com.flink.config.KafkaConfig;
import com.flink.feature.BaseSource;
import com.flink.feature.BaseSourceTable;
import com.flink.feature.FlinkFeatureService;
import com.flink.feature.TimestampsAndWatermarks;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Tumble.over;


public class FlinkSoSKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = ExecutionEnvUtil.prepare();
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // Flink 读取 kafka 数据
        DataStreamSource<String> data = KafkaConfig.buildSource(streamEnv);
//        //处理加工逻辑  数据 加工

        SingleOutputStreamOperator<BaseSource> tempData = data.map(new FlinkFeatureService()).assignTimestampsAndWatermarks(new TimestampsAndWatermarks());
        ;
//        Table tableAll = tableEnv.fromDataStream(tempData,"content, eventTime.proctime, logType");

        Table table = tableEnv.fromDataStream(tempData, "logType,content,eventTime,rowTime.rowtime");
        tableEnv.registerTable("all_table", table);
//       tableEnv.sqlQuery("select logType,content,eventTime from all_table").execute().print();
        Table wordWithCount = tableEnv.sqlQuery("SELECT logType, count(logType) FROM all_table GROUP BY logType");
        tableEnv.toRetractStream(wordWithCount, Row.class).print();
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(sorted, Row.class);
        //    tableEnv.scan("tableAll").window(Tumble.over(lit(5).minutes()).on($("eventTime")).as("w"))
        //             .groupBy($("logType"),$("w")).select("logType,count(logType) as cnt").execute().print();

//        tableEnv.toRetractStream(tableAll,BaseSourceTable.class);
//        Table wordCount = tableEnv.sqlQuery("SELECT count(eventTime) AS _count,logType FROM tableAll GROUP BY logType");
//
//        wordCount.insertInto("sinkTable");
//        Table audit_linuxserver_process = tableAll.filter("log_type = 'process'");
//        TableResult res = audit_linuxserver_process.execute();
//        res.print();
        streamEnv.execute("Blink Stream SQL Job");
    }

}
