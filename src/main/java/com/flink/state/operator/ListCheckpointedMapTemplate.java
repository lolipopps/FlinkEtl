package com.flink.state.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ListCheckpointedMapTemplate implements MapFunction<Long, String>,
        ListCheckpointed<Long> {

    public static Logger LOG = LoggerFactory.getLogger(ListCheckpointedMapTemplate.class);

    private List<Long> bufferedElements;

    public ListCheckpointedMapTemplate() {
        this.bufferedElements = new LinkedList<>();
    }


    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) {
        LOG.info("{}: 当前快照编号:{} ,数据 :{}", Thread.currentThread().getName(), checkpointId, bufferedElements);
        return bufferedElements;
    }


    @Override
    public void restoreState(List<Long> state) {
        bufferedElements = state;
        LOG.info("恢复数据 {} 当前 快照数据 :{{}", Thread.currentThread().getName(), state);
    }


    @Override
    public String map(Long value) {
        int size = bufferedElements.size();
        if (size >= 10) {
            for (int i = 0; i < size - 9; i++) {
                Long poll = bufferedElements.remove(0);
                LOG.info("删除过期数据 :{}", poll);
            }
        }
        bufferedElements.add(value);
        int seconds = Calendar.getInstance().get(Calendar.SECOND);
        if (seconds >= 50 && seconds <= 51) {
            int i = 1 / 0;
        }
        LOG.info("{} map data :{}", Thread.currentThread().getName(), bufferedElements);
        return "集合中第一个元素是:" + bufferedElements.get(0) +
                "集合中最后一个元素是:" + bufferedElements.get(bufferedElements.size() - 1) +
                " length is :" + bufferedElements.size();
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(2);
        String path = "file:///home/intsmaze/flink/check/CheckpointedFunctionTemplate";
        FsStateBackend stateBackend = new FsStateBackend(path);
        env.setStateBackend(stateBackend);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));

        env.addSource(new CustomSource())
                .setParallelism(1)
                .map(new ListCheckpointedMapTemplate())
                .print("输出结果");

        env.execute("Intsmaze CheckpointedFunctionTemplate");
    }



    public static class CustomSource extends RichSourceFunction<Long> {

        public Logger LOG = LoggerFactory.getLogger(CheckpointedMapTemplate.CustomSource.class);

        private static final long serialVersionUID = 1L;

        /**

         *
         
         * @date: 2020/10/15 18:33
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            Thread.sleep(10000);
        }

        /**

         *
         
         * @date: 2020/10/15 18:33
         */
        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            Long offset = 0L;
            while (true) {
                LOG.info("{}{}{}", Thread.currentThread().getName(), ":发送数据:", offset);
                ctx.collect(offset);
                offset += 1;
                Thread.sleep(1000);
            }
        }

        /**

         *
         
         * @date: 2020/10/15 18:33
         */
        @Override
        public void cancel() {
        }
    }

}