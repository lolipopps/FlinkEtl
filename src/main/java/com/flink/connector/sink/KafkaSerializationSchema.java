package com.flink.connector.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;


public class KafkaSerializationSchema implements SerializationSchema<Tuple2<Integer, String>> {


    @Override
    public byte[] serialize(Tuple2<Integer, String> element) {
        return element.f1.getBytes();
    }
}
