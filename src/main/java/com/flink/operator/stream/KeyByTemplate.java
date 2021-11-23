package com.flink.operator.stream;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**

 */
public class KeyByTemplate {

    /**
 
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        list.add(new Tuple2<>(1, 11));
        list.add(new Tuple2<>(1, 22));
        list.add(new Tuple2<>(3, 33));
        list.add(new Tuple2<>(5, 55));

        DataStream<Tuple2<Integer, Integer>> dataStream = env.fromCollection(list);

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = dataStream.keyBy(0);

        keyedStream.print("输出结果");

        env.execute("KeyByTemplate");
    }

    /**
 
     */
    @Test
    public void testKeyWithPOJO() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Person, Integer>> list = new ArrayList<Tuple2<Person, Integer>>();
        list.add(new Tuple2(new Person("张三", 28), 1));
        list.add(new Tuple2(new Person("张三", 28), 2));
        list.add(new Tuple2(new Person("李四", 20), 33));

        DataStream<Tuple2<Person, Integer>> dataStream = env
                .fromCollection(list)
                .keyBy("f0");
        dataStream.print();
        env.execute("testKeyWithPOJO");
    }


}
