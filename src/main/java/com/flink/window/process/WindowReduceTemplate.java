package com.flink.window.process;

import com.flink.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



public class WindowReduceTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));

        DataStream<Tuple3<String, Integer, String>> reduce = streamSource.keyBy("f0")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((new ReduceFunction<Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> value1,
                                                                  Tuple3<String, Integer, String> value2) {
                        return value1.f1 > value2.f1 ? value2 : value1;
                    }
                }));

        reduce.print();

        env.execute("WindowReduceTemplate");
    }

}
