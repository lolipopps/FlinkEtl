package com.flink.param;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
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