package com.flink.window.time.watermark;


import com.flink.window.time.bean.EventBean;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


public class EventTimePunctuatedWaterMarks implements AssignerWithPunctuatedWatermarks<EventBean> {


    @Override
    public long extractTimestamp(EventBean element, long previousElementTimestamp) {
        long timestamp = element.getTime();
        return timestamp;
    }


    @Override
    public Watermark checkAndGetNextWatermark(EventBean lastElement, long extractedTimestamp) {
        long watermark = System.currentTimeMillis();
        if (lastElement.getList().get(0).indexOf("late") < 0) {
            return new Watermark(watermark);
        }
        return null;
    }
}