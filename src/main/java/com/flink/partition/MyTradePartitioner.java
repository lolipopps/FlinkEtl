package com.flink.partition;

import com.flink.bean.Trade;
import org.apache.flink.api.common.functions.Partitioner;



public class MyTradePartitioner implements Partitioner<Trade> {


    @Override
    public int partition(Trade key, int numPartitions) {
        if (key.getCardNum().indexOf("185") >= 0 && key.getTrade() > 1000) {
            return 0;
        } else if (key.getCardNum().indexOf("155") >= 0 && key.getTrade() > 1150) {
            return 1;
        } else {
            return 2;
        }
    }
}