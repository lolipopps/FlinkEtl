package com.alibaba.alink.kafka;


import com.alibaba.alink.operator.common.io.kafka.BaseKafkaSinkBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.types.Row;

public final class Kafka011SinkBuilder extends BaseKafkaSinkBuilder {
    @Override
    public RichSinkFunction<Row> build() {
        SerializationSchema<Row> serializationSchema = getSerializationSchema();
        return new FlinkKafkaProducer011<Row>(topic, serializationSchema, properties);
    }
}