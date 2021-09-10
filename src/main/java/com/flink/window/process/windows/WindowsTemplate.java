package com.flink.window.process.windows;


import com.flink.window.source.SourceForWindow;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**

 * @date: 2020/10/15 18:33
 */
public class WindowsTemplate {

    /**

     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000, true));

        KeyedStream<Tuple3<String, Integer, String>, Tuple> keyedStream = streamSource.keyBy("f0");

//        WindowedStream<Tuple3<String, Integer, String>, Tuple, TimeWindow> windowedStream =keyedStream
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)));

//        WindowedStream<Tuple3<String, Integer, String>, Tuple, TimeWindow> windowedStream =keyedStream
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3)));

//        WindowedStream<Tuple3<String, Integer, String>, Tuple, TimeWindow> windowedStream = keyedStream
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(8)));

        WindowedStream<Tuple3<String, Integer, String>, Tuple, GlobalWindow> windowedStream = keyedStream
                .window(GlobalWindows.create()).trigger(CountTrigger.of(3));

        windowedStream.sum("f1").print("窗口中元素求和结果");

        env.execute("WindowsTemplate");

    }


}