package com.flink.connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;


public class KafkaSourceSinkTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties conProperties = new Properties();
        conProperties.setProperty("bootstrap.servers", "hyt2:9092");
        conProperties.setProperty("group.id", "intsmaze");
        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(
                "flink_orders3", new SimpleStringSchema(), conProperties);

        DataStream<String> streamSource = env.addSource(kafkaConsumer);

        DataStream<String> mapStream = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {

                Thread.sleep(100);
                System.out.println("number：" + value);
                return String.valueOf(value);
            }
        });



        mapStream.print();


        env.execute("KafkaSourceSinkTemplate");
    }
}
