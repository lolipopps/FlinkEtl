package com.flink.window.process;

import com.flink.window.source.SourceForWindow;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**

 * @date: 2020/10/15 18:33
 */
public class ApplyWindowTemplate {

    /**

     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));

        DataStream<String> applyStream = streamSource
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new UserDefinedWindowFunction());

        applyStream.print("输出结果");

        env.execute("ApplyWindowTemplate");
    }

    /**

     * @date: 2020/10/15 18:33
     */
    public static class UserDefinedWindowFunction implements WindowFunction<Tuple3<String, Integer, String>, String, String, TimeWindow> {

        /**

         * @date: 2020/10/15 18:33
         */
        @Override
        public void apply(String key, TimeWindow window, Iterable<Tuple3<String, Integer, String>> input,
                          Collector<String> out) {
            String str = "";
            long count = 0;
            for (Tuple3<String, Integer, String> in : input) {
                str = StringUtils.join(str, in.toString());
                count++;
            }
            out.collect("Window: " + window.toString() + " key:" + key + "  count: " + count);
        }
    }

}


