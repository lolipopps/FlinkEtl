package com.alibaba.alink.kafka;


import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.kafka.para.Kafka011SinkParams;
import com.alibaba.alink.operator.common.io.kafka.BaseKafkaSinkBuilder;

import com.alibaba.alink.operator.stream.sink.BaseKafkaSinkStreamOp;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Data sink for kafka 0.11.x.
 */
@IoOpAnnotation(name = "kafka011", ioType = IOType.SinkStream)
public final class Kafka011SinkStreamOp extends BaseKafkaSinkStreamOp<Kafka011SinkStreamOp>
        implements Kafka011SinkParams<Kafka011SinkStreamOp> {


    public Kafka011SinkStreamOp() {
        this(new Params());
    }

    public Kafka011SinkStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(Kafka011SinkStreamOp.class), params);
    }

    @Override
    protected BaseKafkaSinkBuilder getKafkaSinkBuilder() {
        return new Kafka011SinkBuilder();
    }
}