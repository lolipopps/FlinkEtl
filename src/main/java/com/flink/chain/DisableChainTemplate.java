package com.flink.chain;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class DisableChainTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.disableOperatorChaining();

        DataStream<Tuple2<String, Integer>> inputStream = env.addSource(new ChainSource());

        DataStream<Tuple2<String, Integer>> filter = inputStream.filter(new RichFilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) {
                System.out.println("filter操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                return true;
            }
        });

        DataStream<Tuple2<String, Integer>> mapOne = filter.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
                System.out.println("map-one操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                return value;
            }
        });

//        mapOne = ((SingleOutputStreamOperator<Tuple2<String,Integer>>) mapOne).disableChaining();

        DataStream<Tuple2<String, Integer>> mapTwo = mapOne.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
                System.out.println("map-two操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                return value;
            }
        });
        mapTwo.print();

        env.execute("disableChaining");
    }

}