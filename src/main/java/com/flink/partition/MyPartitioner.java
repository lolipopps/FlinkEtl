package com.flink.partition;

import org.apache.flink.api.common.functions.Partitioner;



public class MyPartitioner implements Partitioner<String> {


    @Override
    public int partition(String key, int numPartitions) {
        if (key.indexOf("185") >= 0) {
            return 0;
        } else if (key.indexOf("155") >= 0) {
            return 1;
        } else {
            return 2;
        }
    }
}