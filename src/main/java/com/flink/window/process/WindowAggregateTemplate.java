package com.flink.window.process;

import com.flink.window.source.SourceForWindow;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class WindowAggregateTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));

        DataStream<Tuple3<String, Integer, String>> sum = streamSource.keyBy("f0")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        sum.print();

        env.execute("WindowAggregateTemplate");
    }

}
