package com.flink.sqlapi;

import com.flink.bean.Order;
import com.flink.bean.RateBean;
import com.flink.sql.PrepareData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;


public class JoinWithTemporalTable {



    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<RateBean> ratesHistory = env.fromCollection(PrepareData.getRateData());
        ratesHistory = ratesHistory.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<RateBean>() {
            /**
             * github地址: https://github.com/intsmaze
             * 博客地址：https://www.cnblogs.com/intsmaze/
             * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
             *
             * @auther: intsmaze(刘洋)
             * @date: 2020/10/15 18:33
             */
            @Override
            public long extractTimestamp(RateBean element, long previousElementTimestamp) {
                return element.getTime().getTime();
            }

            /**
             * github地址: https://github.com/intsmaze
             * 博客地址：https://www.cnblogs.com/intsmaze/
             * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
             *
             * @auther: intsmaze(刘洋)
             * @date: 2020/10/15 18:33
             */
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }
        });

        StreamTableEnvironment tEnv = null;
        Table table = tEnv.fromDataStream(ratesHistory, "id,currency,time.rowtime,rate");
        tEnv.registerTable("RatesHistory", table);

        TemporalTableFunction fun = table.createTemporalTableFunction("time", "currency");
        tEnv.registerFunction("Rates", fun);

        DataStream<Order> orders = env.fromCollection(PrepareData.getOrderData());
        orders = orders.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<Order>() {
                    /**
                     * github地址: https://github.com/intsmaze
                     * 博客地址：https://www.cnblogs.com/intsmaze/
                     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
                     *
                     * @auther: intsmaze(刘洋)
                     * @date: 2020/10/15 18:33
                     */
                    @Override
                    public long extractTimestamp(Order element, long previousElementTimestamp) {
                        return element.getTime().getTime();
                    }

                    /**
                     * github地址: https://github.com/intsmaze
                     * 博客地址：https://www.cnblogs.com/intsmaze/
                     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
                     *
                     * @auther: intsmaze(刘洋)
                     * @date: 2020/10/15 18:33
                     */
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }
                });

        tEnv.registerDataStream("Orders", orders, "id,currency,orderTime.rowtime,amount");

        Table sqlResult = tEnv.sqlQuery("SELECT o.id,r.id,o.amount * r.rate AS amount " +
                "FROM Orders AS o,LATERAL TABLE (Rates(o.orderTime)) AS r " +
                "WHERE r.currency = o.currency");

        tEnv.toAppendStream(sqlResult, Row.class).print();

        env.execute();
    }


}
