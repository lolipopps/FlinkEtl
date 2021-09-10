package com.flink.partition;

import com.flink.bean.Trade;
import org.apache.flink.api.common.functions.Partitioner;


/**

 * @date: 2020/10/15 18:33
 */
public class MyTradePartitioner implements Partitioner<Trade> {

    /**

     * @date: 2020/10/15 18:33
     */
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