package com.flink.operator.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterTemplate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> streamSource = env.generateSequence(1, 5);
        DataStream<Long> filterStream = streamSource.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                if (value == 2L || value == 4L) {
                    return false;
                }
                return true;
            }
        });
        filterStream.print("输出结果");
        env.execute("Filter Template");
    }
}