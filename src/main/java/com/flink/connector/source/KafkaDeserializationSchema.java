package com.flink.connector.source;

import com.flink.bean.SchemaBean;
import com.google.gson.Gson;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;


public class KafkaDeserializationSchema extends AbstractDeserializationSchema<SchemaBean> {


    @Override
    public SchemaBean deserialize(byte[] message) {
        Gson gson = new Gson();
        return gson.fromJson(new String(message), SchemaBean.class);
    }

}
