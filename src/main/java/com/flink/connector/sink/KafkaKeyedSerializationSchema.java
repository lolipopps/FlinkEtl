package com.flink.connector.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class KafkaKeyedSerializationSchema implements KeyedSerializationSchema<Tuple2<Integer, String>> {


    @Override
    public byte[] serializeKey(Tuple2<Integer, String> element) {
        return element.f0.toString().getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2<Integer, String> element) {
        return element.f1.getBytes();
    }

    @Override
    public String getTargetTopic(Tuple2<Integer, String> element) {
        if (element.f0 > 30) {
            return "intsmaze-discover-2";
        }
        return null;
    }
}
