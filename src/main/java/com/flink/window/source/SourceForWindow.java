package com.flink.window.source;


import com.flink.window.util.TimeUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SourceForWindow implements SourceFunction<Tuple3<String, Integer, String>> {

    public static Logger LOG = LoggerFactory.getLogger(SourceForWindow.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private long sleepTime;

    private Boolean stopSession = false;


    public SourceForWindow(long sleepTime, Boolean stopSession) {
        this.sleepTime = sleepTime;
        this.stopSession = stopSession;
    }


    public SourceForWindow(long sleepTime) {
        this.sleepTime = sleepTime;
    }


    @Override
    public void run(SourceContext<Tuple3<String, Integer, String>> ctx) throws Exception {
        int count = 0;
        while (isRunning) {
            String word = WORDS[count % WORDS.length];
            String time = TimeUtils.getHHmmss(System.currentTimeMillis());
            Tuple3<String, Integer, String> tuple2 = Tuple3.of(word, count, time);
            ctx.collect(tuple2);
            LOG.info("send data :" + tuple2);
            System.out.println("send data :" + tuple2);
            if (stopSession && count == WORDS.length) {
                Thread.sleep(10000);
            } else {
                Thread.sleep(sleepTime);
            }
            count++;
        }
    }


    @Override
    public void cancel() {
        isRunning = false;
    }



    public static final String[] WORDS = new String[]{
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "java",
            "flink",
            "flink",
            "flink",
            "intsmaze",
            "intsmaze",
            "hadoop",
            "hadoop",
            "spark"
    };
}