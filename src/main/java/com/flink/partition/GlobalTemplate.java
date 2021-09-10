package com.flink.partition;

import com.flink.bean.Trade;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**

 * @date: 2020/10/15 18:33
 */
public class GlobalTemplate {

    /**

     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        final String flag = " global分区策略前子任务名称:";

        DataStream<Trade> inputStream = env.addSource(new  PartitionSource());

        DataStream<Trade> mapOne = inputStream.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                System.out.println("元素值:" + value + flag + getRuntimeContext().getTaskNameWithSubtasks()
                        + " ,子任务编号:" + getRuntimeContext().getIndexOfThisSubtask());
                return value;
            }
        });

        DataStream<Trade> mapTwo = mapOne.global();

        DataStream<Trade> mapThree = mapTwo.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                System.out.println("元素值:" + value + " global分区策略后子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks()
                        + " ,子任务编号:" + getRuntimeContext().getIndexOfThisSubtask());
                return value;
            }
        }).setParallelism(2);
        mapThree.print();

        env.execute("Physical partitioning");
    }
}
