package com.flink.window.process.evitor;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**

 * @date: 2020/10/15 18:33
 */
public class CountEvictorDebug<W extends Window> implements Evictor<Object, W> {
    private static final long serialVersionUID = 1L;

    private final long maxCount;
    private final boolean doEvictAfter;

    /**

     * @date: 2020/10/15 18:33
     */
    private CountEvictorDebug(long count, boolean doEvictAfter) {
        this.maxCount = count;
        this.doEvictAfter = doEvictAfter;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    private CountEvictorDebug(long count) {
        this.maxCount = count;
        this.doEvictAfter = false;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (!doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    /**

     * @date: 2020/10/15 18:33
     */
    private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
        if (size <= maxCount) {
            return;
        } else {
            int evictedCount = 0;
            for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) {
                iterator.next();
                evictedCount++;
                if (evictedCount > size - maxCount) {
                    break;
                } else {
                    iterator.remove();
                }
            }
        }
    }

    /**

     * @date: 2020/10/15 18:33
     */
    public static <W extends Window> CountEvictorDebug<W> of(long maxCount) {
        return new CountEvictorDebug<>(maxCount);
    }

    /**

     * @date: 2020/10/15 18:33
     */
    public static <W extends Window> CountEvictorDebug<W> of(long maxCount, boolean doEvictAfter) {
        return new CountEvictorDebug<>(maxCount, doEvictAfter);
    }
}
