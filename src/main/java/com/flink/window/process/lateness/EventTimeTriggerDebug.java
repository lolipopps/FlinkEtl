package com.flink.window.process.lateness;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**

 * @date: 2020/10/15 18:33
 */
public class EventTimeTriggerDebug extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private EventTimeTriggerDebug() {
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ?
                TriggerResult.FIRE :
                TriggerResult.CONTINUE;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public boolean canMerge() {
        return true;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**

     * @date: 2020/10/15 18:33
     */
    public static EventTimeTriggerDebug create() {
        return new EventTimeTriggerDebug();
    }
}

