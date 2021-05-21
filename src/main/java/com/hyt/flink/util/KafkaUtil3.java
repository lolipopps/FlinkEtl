package com.hyt.flink.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hyt.flink.config.KafkaConfig;
import com.hyt.flink.config.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
public class KafkaUtil3 {
    public static void writeToKafka() throws InterruptedException, IOException {
        Properties props = null;
        String[] topics = {"streamA","streamB"};
        props = KafkaConfig.buildKafkaProps();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        Random ra = new Random();
        while (true) {
            InputStream inputStream = ExecutionEnvUtil.class.getClassLoader().getResourceAsStream("./test.csv");
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }

            String[] ress = result.toString("UTF-8").split("\n");
            for (String re : ress) {
                    String data = re+"," + DateUtil.getCurrentTime();
                 int num = ra.nextInt(2);
                    ProducerRecord record = new ProducerRecord<String, String>(topics[num], null, null, data);
                    producer.send(record);
                    log.info(topics[num] + " 发送数据: " + record.toString());
                    Thread.sleep(100);

            }
            producer.flush();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        writeToKafka();
    }
}
