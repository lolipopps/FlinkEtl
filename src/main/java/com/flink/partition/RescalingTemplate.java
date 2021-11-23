package com.flink.partition;


import com.flink.bean.Trade;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


public class RescalingTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        final String flag = " rescale分区策略前子任务名称:";

        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("185XXX", 899, "2018"));
        list.add(new Trade("155XXX", 1111, "2019"));
        list.add(new Trade("155XXX", 1199, "2019"));
        list.add(new Trade("185XXX", 899, "2018"));
        list.add(new Trade("138XXX", 19, "2019"));
        list.add(new Trade("138XXX", 399, "2020"));
        list.add(new Trade("138XXX", 399, "2020"));
        list.add(new Trade("138XXX", 399, "2020"));

        DataStream<Trade> inputStream = env.fromCollection(list);

        DataStream<Trade> mapOne = inputStream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                if (key.indexOf("185") >= 0) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }, "cardNum");

        DataStream<Trade> mapTwo = mapOne.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                System.out.println("元素值:" + value + flag + getRuntimeContext().getTaskNameWithSubtasks()
                        + " ,子任务编号:" + getRuntimeContext().getIndexOfThisSubtask());
                return value;
            }
        }).setParallelism(2);

        DataStream<Trade> mapThree = mapTwo.rescale();

        DataStream<Trade> mapFour = mapThree.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                System.out.println("元素值:" + value + " rescale分区策略后子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks()
                        + " ,子任务编号:" + getRuntimeContext().getIndexOfThisSubtask());
                return value;
            }
        });
        mapFour.print();

        env.execute("Rescaling partitioning");
    }
}
