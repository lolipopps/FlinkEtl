package com.flink.connector.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomSourceTemplate implements SourceFunction<Long> {

    public static Logger LOG = LoggerFactory.getLogger(CustomSourceTemplate.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;
    private long counter = 0;

    private long sleepTime;

    public CustomSourceTemplate(long sleepTime) {
        this.sleepTime = sleepTime;
    }


    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(counter);
            System.out.println("send data :" + counter);
            LOG.info("send data :" + counter);
            counter++;
            Thread.sleep(sleepTime);
        }
    }


    @Override
    public void cancel() {
        LOG.warn("接收到取消任务的命令.......................");
        isRunning = true;
    }


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        DataStream<Long> inputStream =
                env.addSource(new CustomSourceTemplate(100))
                        .setParallelism(1);

        DataStream<Long> inputStream1 = inputStream
                .map((Long values) -> {
                    return values + System.currentTimeMillis();
                });
        inputStream1.print();

        env.execute("Intsmaze Custom Source");
    }
}