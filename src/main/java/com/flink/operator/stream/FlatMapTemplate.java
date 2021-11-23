package com.flink.operator.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**

 */
public class FlatMapTemplate {

    /**
 
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> streamSource = env.fromElements(
                new Tuple2<>("liu yang", 1),
                new Tuple2<>("my blog is intsmaze", 2),
                new Tuple2<>("hello flink", 2));

        DataStream<Tuple1<String>> resultStream = streamSource
                .flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple1<String>>() {
                    @Override
                    public void flatMap(Tuple2<String, Integer> value,
                                        Collector<Tuple1<String>> out) {

                        if ("liu yang".equals(value.f0)) {
                            return;
                        } else if (value.f0.indexOf("intsmaze") >= 0) {
                            for (String word : value.f0.split(" ")) {
                                out.collect(Tuple1.of("Split intsmaze：" + word));
                            }
                        } else {
                            out.collect(Tuple1.of("Not included intsmaze：" + value.f0));
                        }
                    }
                });
        resultStream.print("输出结果");

        env.execute("FlatMapTemplate");
    }

}