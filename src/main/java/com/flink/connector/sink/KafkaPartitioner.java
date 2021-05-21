package com.flink.connector.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;


public class KafkaPartitioner extends FlinkKafkaPartitioner<Tuple2<Integer, String>> {


    @Override
    public int partition(Tuple2<Integer, String> record, byte[] key, byte[] value, String targetTopic, int[] partitions) {

        System.out.println("数据流中的元素:" + record.toString());
        System.out.println("消息的key值:" + new String(key));
        System.out.println("消息的主体内容：" + new String(value));
        System.out.println("targetTopic:" + targetTopic);

        String sKey = new String(key);
        Integer integer = Integer.valueOf(sKey);
        if (integer < 100) {
            return partitions.length - 1;
        } else {
            return partitions.length - 2;
        }
    }
}
