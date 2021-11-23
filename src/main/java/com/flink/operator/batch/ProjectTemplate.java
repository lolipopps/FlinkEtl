package com.flink.operator.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;


public class ProjectTemplate {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, Integer, String>> list = new ArrayList<Tuple3<String, Integer, String>>();
        list.add(new Tuple3("185XXX", 899, "周一"));
        list.add(new Tuple3("155XXX", 1199, "周二"));
        list.add(new Tuple3("138XXX", 19, "周三"));

        DataSet<Tuple3<String, Integer, String>> dataSet = env.fromCollection(list);

        DataSet<Tuple2<String, String>> result = dataSet.project(2, 0);
        result.print("输出结果");
        env.execute("Project Template");
    }
}
