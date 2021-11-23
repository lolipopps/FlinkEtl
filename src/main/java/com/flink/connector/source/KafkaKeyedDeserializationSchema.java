package com.flink.connector.source;

import com.flink.bean.SchemaBean;
import com.google.gson.Gson;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;



public class KafkaKeyedDeserializationSchema implements KeyedDeserializationSchema<Tuple2<String, SchemaBean>> {


    @Override
    public Tuple2<String, SchemaBean> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
        Gson gson = new Gson();
        String key = gson.fromJson(new String(messageKey), String.class);
        SchemaBean value = gson.fromJson(new String(message), SchemaBean.class);
        return Tuple2.of(key, value);
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, SchemaBean> nextElement) {
        return false;
    }


    @Override
    public TypeInformation<Tuple2<String, SchemaBean>> getProducedType() {
        return new TypeHint<Tuple2<String, SchemaBean>>() {
        }.getTypeInfo();
    }
}
