package com.flink.partition;

import com.flink.bean.Trade;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;


public class PartitionSource extends RichSourceFunction<Trade> {

    private static final long serialVersionUID = 1L;


    @Override
    public void run(SourceContext<Trade> ctx) {
        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("185XXX", 899, "2018"));
        list.add(new Trade("155XXX", 1111, "2019"));
        list.add(new Trade("155XXX", 1199, "2019"));
        list.add(new Trade("185XXX", 899, "2018"));
        list.add(new Trade("138XXX", 19, "2019"));
        list.add(new Trade("138XXX", 399, "2020"));

        for (int i = 0; i < list.size(); i++) {
            Trade trade = list.get(i);
            ctx.collect(trade);
        }
        String subtaskName = getRuntimeContext().getTaskNameWithSubtasks();
        System.out.println("source操作所属子任务名称:" + subtaskName);
    }


    @Override
    public void cancel() {
    }
}
