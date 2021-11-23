package com.flink.operator.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**

 */
public class UnionTemplate {

    /**
 
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> dataStream = env.generateSequence(1, 2);

        DataStream<Long> otherStream = env.generateSequence(1001, 1002);

        DataStream<Long> union = dataStream.union(otherStream);
        union.print("输出结果");
        env.execute("Union Template");
    }
}
