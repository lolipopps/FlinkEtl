package com.flink.sqlapi.groupwindows;

import com.flink.util.TimeUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class EventTimeWaterMarks implements AssignerWithPeriodicWatermarks<Row> {


    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
        Timestamp timestamp = (Timestamp) element.getField(2);
        return timestamp.getTime();
    }

    @Override
    public Watermark getCurrentWatermark() {
        String offset = TimeUtils.getSS(System.currentTimeMillis());
        System.out.println("返回水印------" + offset + "--" + System.currentTimeMillis());
        return new Watermark(System.currentTimeMillis() - 5000);
    }
}