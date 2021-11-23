package com.flink.operator.stream;

import com.flink.bean.Trade;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**

 */
public class AggregationsTemplate {

    /**
 
     */
    @Test
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("188XXX", 30, "2018-07"));
        list.add(new Trade("188XXX", 20, "2018-11"));
        list.add(new Trade("158XXX", 1, "2018-07"));
        list.add(new Trade("158XXX", 2, "2018-06"));
        DataStream<Trade> streamSource = env.fromCollection(list);

        KeyedStream<Trade, Tuple> keyedStream = streamSource.keyBy("cardNum");

        keyedStream.sum("trade").print("sum");

        keyedStream.min("trade").print("min");

        keyedStream.minBy("trade").print("minBy");

        env.execute("Aggregations Template");

    }

}