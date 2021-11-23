package com.flink.state.savepoint;


import com.flink.state.operator.CheckpointedMapTemplate;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SavePointedTemplate {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(20000);

        String path = "./checkpoint";
        StateBackend stateBackend = new RocksDBStateBackend(path);
        env.setStateBackend(stateBackend);

        DataStream<Long> sourceStream =
                env.addSource(new CheckpointedMapTemplate.CustomSource());

        sourceStream.map(new CheckpointedMapTemplate(false, false))
                .uid("map-id")
                .print();

        env.execute("Intsmaze SavePointedTemplate");
    }
}