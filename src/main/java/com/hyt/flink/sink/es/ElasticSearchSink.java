package com.hyt.flink.sink.es;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.hyt.flink.config.ElasticConfig;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.types.Row;
import java.net.MalformedURLException;
import static com.hyt.flink.config.PropertiesConstants.STREAM_SINK_PARALLELISM;
import static com.hyt.flink.util.ExecutionEnvUtil.PARAMETER_TOOL;

public class ElasticSearchSink {
    public static <T> void addSink( StreamOperator dataStream,ElasticsearchSinkFunction<T> func) throws MalformedURLException {
        ElasticsearchSink<Row> es = ElasticConfig.buildSink(func);
        int parallelism = PARAMETER_TOOL.getInt(STREAM_SINK_PARALLELISM, 5);
        dataStream.getDataStream().addSink(es).setParallelism(parallelism);
    }

}
