package com.flink.operator.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class FlatMapTemplate {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, Integer>> dataSet = env.fromElements(
                new Tuple2<>("liu yang", 1),
                new Tuple2<>("my blog is intsmaze", 2),
                new Tuple2<>("hello flink", 2));

        DataSet<Tuple1<String>> flatMapDataSet = dataSet.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple1<String>>() {
            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Tuple1<String>> out) {

                if ("liu yang".equals(value.f0)) {
                    return;
                } else {
                    out.collect(new Tuple1<String>("Not included intsmaze：" + value.f0));
                }
            }
        });
        flatMapDataSet.print("输出结果");

        env.execute("FlatMapTemplate");

    }
}