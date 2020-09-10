package com.alibaba.alink.kafka;

import com.alibaba.alink.operator.common.io.kafka.BaseKafkaSourceBuilder;
import com.alibaba.alink.operator.common.io.kafka.KafkaMessageDeserialization;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

import static com.alibaba.alink.operator.common.io.kafka.KafkaMessageDeserialization.KAFKA_SRC_FIELD_NAMES;
import static com.alibaba.alink.operator.common.io.kafka.KafkaMessageDeserialization.KAFKA_SRC_FIELD_TYPES;

public final class MyKafka011SourceBuilder extends BaseKafkaSourceBuilder {

    private static class MessageDeserialization implements KafkaDeserializationSchema<Row> {
        @Override
        public boolean isEndOfStream(Row nextElement) {
            return false;
        }

        @Override
        public Row deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            return KafkaMessageDeserialization.deserialize(
                    record.key(), record.value(), record.topic(), record.partition(), record.offset()
            );
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return new RowTypeInfo(KAFKA_SRC_FIELD_TYPES, KAFKA_SRC_FIELD_NAMES);
        }
    }

    @Override
    public RichParallelSourceFunction<Row> build() {
        FlinkKafkaConsumer011<Row> consumer;
        if (!StringUtils.isNullOrWhitespaceOnly(topicPattern)) {
            Pattern pattern = Pattern.compile(topicPattern);
            consumer = new FlinkKafkaConsumer011<Row>(pattern, new MessageDeserialization(), properties);
        } else {
            consumer = new FlinkKafkaConsumer011<Row>(topic, new MessageDeserialization(), properties);
        }
        switch (super.startupMode) {
            case LATEST: {
                consumer.setStartFromLatest();
                break;
            }
            case EARLIEST: {
                consumer.setStartFromEarliest();
                break;
            }
            case GROUP_OFFSETS: {
                consumer.setStartFromGroupOffsets();
                break;
            }
            case TIMESTAMP: {
                consumer.setStartFromTimestamp(startTimeMs);
                break;
            }
            default: {
                throw new IllegalArgumentException("invalid startupMode.");
            }
        }

        return consumer;
    }
}