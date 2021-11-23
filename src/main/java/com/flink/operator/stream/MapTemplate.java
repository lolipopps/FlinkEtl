package com.flink.operator.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**

 */
public class MapTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> streamSource = env.generateSequence(1, 5);

        DataStream<Tuple2<Long, Integer>> mapStream = streamSource
                .map(new MapFunction<Long, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(Long values) {
                        return new Tuple2<>(values * 100, values.hashCode());
                    }
                });
        mapStream.print("输出结果");
        env.execute("MapTemplate");
    }

}