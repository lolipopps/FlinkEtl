package com.flink.window.process;


import com.flink.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


@Deprecated
public class ProcessWindowFoldTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));


        DataStream<Tuple3<String, Integer, String>> fold = streamSource.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .fold(Tuple3.of("Start", 0, "time"), new FoldFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>>() {

                    @Override
                    public Tuple3<String, Integer, String> fold(Tuple3<String, Integer, String> accumulator, Tuple3<String, Integer, String> value) throws Exception {

                        if (value.f1 > accumulator.f1) {
                            accumulator.f1 = value.f1;
                            accumulator.f0 = accumulator.f0 + "--" + value.f0;
                        }
                        return accumulator;
                    }
                });


        fold.print();

        env.execute("TumblingWindows");
    }


    public static class UserDefinedProcessWindow
            extends ProcessWindowFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>, String, TimeWindow> {


        /**

         *
         
         * @date: 2020/10/15 18:33
         */
        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, Integer, String>> elements, Collector<Tuple3<String, Integer, String>> out) throws Exception {

        }
    }
}
