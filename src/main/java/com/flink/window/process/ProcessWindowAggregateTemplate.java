package com.flink.window.process;

import com.flink.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**

 * @date: 2020/10/15 18:33
 */
@Deprecated
public class ProcessWindowAggregateTemplate {

    /**

     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));

        DataStream<String> window = streamSource
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new UserDefindAggregationFunction(), new UserDefinedProcessWindow());

        window.print();
        env.execute("ProcessWindowAggregateTemplate");

    }

    /**

     * @date: 2020/10/15 18:33
     */
    private static class UserDefinedProcessWindow
            extends ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {
        /**

         * @date: 2020/10/15 18:33
         */
        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple2<String, Integer>> values,
                            Collector<String> out) {

            for (Tuple2<String, Integer> in : values) {
                out.collect("Window: " + context.window() + " key:" + key + "  value: " + in.toString());
            }
        }
    }

    /**

     * @date: 2020/10/15 18:33
     */
    private static class UserDefindAggregationFunction
            implements AggregateFunction<Tuple3<String, Integer, String>, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        /**

         * @date: 2020/10/15 18:33
         */
        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return new Tuple2<>("", 0);
        }

        /**

         * @date: 2020/10/15 18:33
         */
        @Override
        public Tuple2<String, Integer> add(Tuple3<String, Integer, String> value, Tuple2<String, Integer> accumulator) {
            accumulator.f0 = accumulator.f0 + "：" + value.f0;
            accumulator.f1 = accumulator.f1 + 1;
            return accumulator;
        }

        /**

         * @date: 2020/10/15 18:33
         */
        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
            return accumulator;
        }

        /**

         * @date: 2020/10/15 18:33
         */
        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> accumulator_a, Tuple2<String, Integer> accumulator_b) {
            accumulator_a.f0 = accumulator_a.f0 + accumulator_b.f0;
            accumulator_a.f1 = accumulator_a.f1 + accumulator_b.f1;
            return accumulator_a;
        }
    }

}


