package com.flink.connector.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;



@Deprecated
public class SinkTemplate {


    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Long> input;


    @Test
    public void testWriteText() throws Exception {
        input = env.fromElements(1L, 21L, 22L);
        input.writeAsText("///home/intsmaze/flink/sink-text.txt");
        input.writeAsText("///home/intsmaze/flink/sink-text.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }


    @Test
    public void testWriteCsv() throws Exception {

        DataStream<Long> input = env.fromElements(1L, 21L, 22L);
//       input.writeAsCsv("///home/intsmaze/flink/sink-text.csv");


        DataStream<Tuple2<Integer, Integer>> inputTuple = env.fromElements(new Tuple2<>(1, 2), new Tuple2<>(11, 22), new Tuple2<>(111, 222));
        inputTuple.writeAsCsv("///home/intsmaze/flink/sink-text.csv");

        env.execute();
    }


    @Test
    public void testPrint() throws Exception {
        env.setParallelism(1);
        DataStream<Long> input = env.fromElements(1L, 21L, 22L);
        input.print("intsmaze--");
        input.printToErr();

        env.execute();
    }

    @Test
    public void testSocket() throws Exception {
        DataStream<String> input = env.fromElements("flink", "streaming");
        input.writeToSocket("127.0.0.1", 9998, new SimpleStringSchema());

        env.execute();
    }

}
