package com.flink.param;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**

 */
public class ConstructorTemplate {

    /**

     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Long> dataStream = env.generateSequence(1, 15);

        ParamBean paramBean = new ParamBean("intsmaze", 10);
        DataStream<Long> resultStream = dataStream
                .filter(new FilterConstructed(paramBean));

        resultStream.print("constructed stream is :");
        env.execute("ParamTemplate intsmaze");
    }

    /**

     */
    private static class FilterConstructed implements FilterFunction<Long> {

        private final ParamBean paramBean;


        public FilterConstructed(ParamBean paramBean) {
            this.paramBean = paramBean;
        }

        @Override
        public boolean filter(Long value) {
            return value > paramBean.getFlag();
        }
    }
}
