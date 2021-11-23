package com.flink.window.source;


import com.flink.window.time.bean.EventBean;
import com.flink.window.util.TimeUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SourceWithTimestamps implements SourceFunction<EventBean> {

    public static Logger LOG = LoggerFactory.getLogger(SourceWithTimestamps.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private int counter = 0;

    private long sleepTime;


    public SourceWithTimestamps(long sleepTime) {
        this.sleepTime = sleepTime;
    }


    @Override
    public void run(SourceContext<EventBean> ctx) throws Exception {
        while (isRunning) {
            if (counter >= 16) {
                isRunning = false;
            } else {
                EventBean bean = Data.BEANS[counter];
                ctx.collect(bean);
                String time = TimeUtils.getHHmmss(System.currentTimeMillis());
                System.out.println("send 元素内容 : [" + bean + " ] now time:" + time);
                if (bean.getList().get(0).indexOf("nosleep") <= 0) {
                    Thread.sleep(sleepTime);
                }
            }
            counter++;
        }
    }


    @Override
    public void cancel() {
        isRunning = false;
    }


}

