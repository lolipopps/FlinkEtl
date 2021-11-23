package com.flink.operator.stream;

import com.flink.bean.Trade;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**

 */
public class SplitTemplate {

    /**
 
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("185XXX", 899, "周一"));
        list.add(new Trade("155XXX", 1199, "周二"));
        list.add(new Trade("138XXX", 19, "周三"));
        DataStream<Trade> dataStream = env.fromCollection(list);

        SplitStream splitStream = dataStream.split(new OutputSelector<Trade>() {
            @Override
            public Iterable<String> select(Trade value) {
                List<String> output = new ArrayList<String>();
                if (value.getTrade() < 100) {
                    output.add("Small amount");
                    output.add("Small amount backup");
                } else if (value.getTrade() > 100) {
                    output.add("Large amount");
                }
                return output;
            }
        });

        splitStream.select("Small amount")
                .print("Small amount:");

        splitStream.select("Large amount").
                print("Large amount:");

        splitStream.select("Small amount backup", "Large amount")
                .print("Small amount backup and Large amount");

        env.execute("SplitTemplate");
    }
}