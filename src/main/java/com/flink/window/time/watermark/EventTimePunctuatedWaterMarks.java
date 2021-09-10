package com.flink.window.time.watermark;


import com.flink.window.time.bean.EventBean;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**

 * @date: 2020/10/15 18:33
 */
public class EventTimePunctuatedWaterMarks implements AssignerWithPunctuatedWatermarks<EventBean> {

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
    public Watermark checkAndGetNextWatermark(EventBean lastElement, long extractedTimestamp) {
        long watermark = System.currentTimeMillis();
        if (lastElement.getList().get(0).indexOf("late") < 0) {
            return new Watermark(watermark);
        }
        return null;
    }
}