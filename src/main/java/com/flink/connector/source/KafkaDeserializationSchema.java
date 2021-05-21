package com.flink.connector.source;

import com.flink.bean.SchemaBean;
import com.google.gson.Gson;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;


public class KafkaDeserializationSchema extends AbstractDeserializationSchema<SchemaBean> {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public SchemaBean deserialize(byte[] message) {
        Gson gson = new Gson();
        return gson.fromJson(new String(message), SchemaBean.class);
    }

}
