package com.flink.window.time.watermark;

import com.flink.window.time.bean.EventBean;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**

 * @date: 2020/10/15 18:33
 */
public class EventTimeWaterMarks implements AssignerWithPeriodicWatermarks<EventBean> {

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public long extractTimestamp(EventBean element, long previousElementTimestamp) {
        long timestamp = element.getTime();
        return timestamp;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public Watermark getCurrentWatermark() {
        long watermark = System.currentTimeMillis();
        return new Watermark(watermark);
    }
}