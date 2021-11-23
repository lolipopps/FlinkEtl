package com.flink.udf.aggre;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class AggregateFunctionTemplate extends AggregateFunction<Double, AccumulatorBean> {



    @Override
    public AccumulatorBean createAccumulator() {
        return new AccumulatorBean();
    }


    @Override
    public Double getValue(AccumulatorBean acc) {
        return acc.totalPrice / acc.totalNum;
    }


    public void accumulate(AccumulatorBean acc, double price, int num) {
        acc.totalPrice += price * num;
        acc.totalNum += num;
    }


    public void retract(AccumulatorBean acc, long iValue, int iWeight) {
        acc.totalPrice -= iValue * iWeight;
        acc.totalNum -= iWeight;
    }


    public void merge(AccumulatorBean acc, Iterable<AccumulatorBean> it) {
        Iterator<AccumulatorBean> iter = it.iterator();
        while (iter.hasNext()) {
            AccumulatorBean a = iter.next();
            acc.totalNum += a.totalNum;
            acc.totalPrice += a.totalPrice;
        }
    }


    public void resetAccumulator(AccumulatorBean acc) {
        acc.totalNum = 0;
        acc.totalPrice = 0L;
    }


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<Row> dataList = new ArrayList<>();
        dataList.add(Row.of("张三", "可乐", 20.0D, 4));
        dataList.add(Row.of("张三", "果汁", 10.0D, 4));
        dataList.add(Row.of("李四", "咖啡", 10.0D, 2));

        DataSource<Row> rowDataSource = env.fromCollection(dataList);

        tEnv.registerDataSet("orders", rowDataSource, "user,name,price, num");

        tEnv.registerFunction("custom_aggregate", new AggregateFunctionTemplate());

        Table sqlResult = tEnv.sqlQuery("SELECT user, custom_aggregate(price, num)  FROM orders GROUP BY user");

        DataSet<Row> result = tEnv.toDataSet(sqlResult, Row.class);
        result.print("result");
        env.execute();
    }
}




