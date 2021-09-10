package com.flink.window.process;


import com.flink.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**

 * @date: 2020/10/15 18:33
 */
public class CustomWindowAggregateTemplate {

    /**

     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));

        streamSource.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new Average()).print();

        env.execute("CustomWindowAggregateTemplate");
    }
}

/**

 * @date: 2020/10/15 18:33
 */
class Average implements AggregateFunction<Tuple3<String, Integer, String>, AverageAccumulator, AverageAccumulator> {

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator();
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        a.setCount(a.getCount() + b.getCount());
        a.setSum(a.getSum() + b.getSum());
        return a;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public AverageAccumulator add(Tuple3<String, Integer, String> value, AverageAccumulator acc) {
        acc.setWord(value.f0);
        acc.setSum(acc.getSum() + value.f1);
        acc.setCount(acc.getCount() + 1);
        return acc;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public AverageAccumulator getResult(AverageAccumulator acc) {
        return acc;
    }
}

/**

 * @date: 2020/10/15 18:33
 */
class AverageAccumulator {

    private String word;

    private long count;
    private long sum;

    public AverageAccumulator() {
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }


    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    @Override
    public String toString() {
        return "AverageAccumulator{" +
                "word='" + word + '\'' +
                ", count=" + count +
                ", sum=" + sum +
                '}';
    }
}
