package com.flink.window.source;


import com.flink.window.time.bean.EventBean;
import com.flink.window.util.TimeUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**

 * @date: 2020/10/15 18:33
 */
public class SourceWithTimestampsWatermarks implements SourceFunction<EventBean> {

    public static Logger LOG = LoggerFactory.getLogger(SourceWithTimestampsWatermarks.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private int counter = 0;

    private long sleepTime;

    /**

     * @date: 2020/10/15 18:33
     */
    public SourceWithTimestampsWatermarks(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public void run(SourceContext<EventBean> ctx) throws Exception {
        while (isRunning) {
            if (counter >= 16) {
                isRunning = false;
            } else {
                EventBean bean = Data.BEANS[counter];
                ctx.collectWithTimestamp(bean, bean.getTime());
                String time = TimeUtils.getHHmmss(System.currentTimeMillis());
                System.out.println("send 元素内容 : [" + bean + " ] now time:" + time);
                if (bean.getList().get(0).indexOf("late") < 0) {
                    ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
                if (bean.getList().get(0).indexOf("nosleep") < 0) {
                    Thread.sleep(sleepTime);
                }
            }
            counter++;
        }
    }

    /**

     * @date: 2020/10/15 18:33
     */
    @Override
    public void cancel() {
        isRunning = false;
    }


}

