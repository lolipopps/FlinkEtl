package com.flink.operator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class UnionTemplate {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> dataSetOne = env.generateSequence(1, 2);

        DataSet<Long> dataSetTwo = env.generateSequence(1001, 1002);

        DataSet<Long> union = dataSetOne.union(dataSetTwo);
        union.print("输出结果");
        env.execute("Union Template");
    }
}
