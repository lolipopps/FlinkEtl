package com.flink.window.time.watermark;

import com.flink.window.time.bean.EventBean;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


public class EventTimeWaterMarks implements AssignerWithPeriodicWatermarks<EventBean> {


    @Override
    public long extractTimestamp(EventBean element, long previousElementTimestamp) {
        long timestamp = element.getTime();
        return timestamp;
    }


    @Override
    public Watermark getCurrentWatermark() {
        long watermark = System.currentTimeMillis();
        return new Watermark(watermark);
    }
}