package com.hyt.flink.util;

import com.hyt.flink.config.KafkaConfig;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

/**
 * @Author: hyt
 * @License: (C) Copyright 2020-2020, xxx Corporation Limited.
 * @Contact: xxx@xxx.com
 * @Version: 1.0
 * @Description: 往kafka中写数据,可以使用这个main函数进行测试
 */
@Slf4j
public class KafkaUtil {
    public static void writeToKafka() throws InterruptedException {
        Properties props = null;
        String topic = "flinkEtl";
        props = KafkaConfig.buildKafkaProps();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        Random ra = new Random();
        while (true) {
            RestElasticUtils restElasticUtils = new RestElasticUtils();
            ArrayList<String> res = restElasticUtils.getAllIndex();
            ArrayList<String> allIndexs = new ArrayList<>();
//            allIndexs.add("system-safe-operation_auth-2020-06-17");
//            allIndexs.add("system-safe-attack_ips-2020-06-17");
//            allIndexs.add("audit-app-otp_user-2020-07-07");
//            allIndexs.add("audit-linuxserver-abnormalprogress-2020-06-28");
//            allIndexs.add("audit-linuxserver-address-2020-06-24");
//            allIndexs.add("audit-linuxserver-file-2020-06-30");
            allIndexs.add("audit-linuxserver-process-2020-06-30");
//            allIndexs.add("audit-linuxserver-network-2020-06");
//
//            allIndexs.add("audit-linuxserver-property-2020-06-30");
//            allIndexs.add("audit-linuxserver-soft-2020-07");
//            allIndexs.add("audit-linuxserver-user-2020-06-30");

            for (String re : allIndexs) {
                ArrayList<String> reData = restElasticUtils.getIndexData(re);
                for (String data : reData) {
                    JsonParser parser = new JsonParser();
                    // 2.获得 根节点元素
                    JsonElement element = parser.parse(data);
                    // 3.根据 文档判断根节点属于 什么类型的 Gson节点对象
                    JsonObject root = element.getAsJsonObject();
                    String send = data;
//                    send = root.get("syslog").getAsString();
//                    if (send == null || send.equals("")) {
//                        data = data;
//                    } else {
//                        data = send;
//                    }
                    ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, data);
                    producer.send(record);
                    // log.info("发送数据: " + record.toString());
                    Thread.sleep(1000);
                }
            }
            producer.flush();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}
