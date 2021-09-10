package com.flink.param;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**

 * @date: 2020/10/15 18:33
 */
public class ParamTemplate {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Long> dataSource = env.generateSequence(1, 10);

        Configuration config = new Configuration();
        config.setInteger("limit", 3);


        DataSet<Long> result = dataSource.filter(new FilterWithParameters())
                .withParameters(config);
        result.print("输出结果");

        env.execute("ParamTemplate");
    }

    private static class FilterWithParameters extends RichFilterFunction<Long> {

        private int limit;


        @Override
        public void open(Configuration parameters) {
            limit = parameters.getInteger("limit", 2);
        }

        @Override
        public boolean filter(Long value) {
            return value > limit;
        }
    }
}