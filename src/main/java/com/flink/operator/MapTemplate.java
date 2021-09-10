package com.flink.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**

 * @date: 2020/10/15 18:33
 */
public class MapTemplate {

    /**

     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> dataSet = env.generateSequence(1, 5);
        DataSet<Tuple2<Long, Integer>> mapDataSet = dataSet.map(new MapFunction<Long, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> map(Long values) {
                return new Tuple2<>(values * 100, values.hashCode());
            }
        });
        mapDataSet.print("输出结果");
        env.execute();
    }

}