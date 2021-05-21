package com.flink.broad;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.List;

public class BroadVariableTemplate {


    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> broadcastDataSet = env.fromElements(1, 2, 3);

        DataSet<String> data = env.fromElements("a", "b");

        DataSet<String> result = data.map(new BroadMapTemplate())
                .withBroadcastSet(broadcastDataSet, "Broadcast Variable Name");

        result.print("输出结果");

        env.execute("BroadVariable Template");
    }


    public static class BroadMapTemplate extends RichMapFunction<String, String> {

        private List<Integer> broadcastCollection;

        @Override
        public void open(Configuration parameters) {
            broadcastCollection = getRuntimeContext()
                    .getBroadcastVariable("Broadcast Variable Name");
        }

        @Override
        public String map(String value) {
            return value + broadcastCollection.toString();
        }
    }
}
