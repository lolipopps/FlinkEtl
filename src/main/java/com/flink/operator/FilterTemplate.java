package com.flink.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**

 * @date: 2020/10/15 18:33
 */
public class FilterTemplate {


    /**

     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Long> dataSet = env.generateSequence(1, 5);

        DataSet<Long> filterDataSet = dataSet.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) {
                if (value == 2L || value == 4L) {
                    return false;
                }
                return true;
            }
        });
        filterDataSet.print("输出结果");
        env.execute("Filter Template");
    }
}