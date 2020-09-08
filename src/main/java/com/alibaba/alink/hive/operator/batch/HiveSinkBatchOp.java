package com.alibaba.alink.hive.operator.batch;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.BaseSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.DBSinkBatchOp;
import com.alibaba.alink.hive.HiveDB;
import com.alibaba.alink.hive.paras.HiveSinkParams;
import org.apache.flink.ml.api.misc.param.Params;


@IoOpAnnotation(name = "hive_batch_sink", ioType = IOType.SinkBatch)
public final class HiveSinkBatchOp extends BaseSinkBatchOp<HiveSinkBatchOp>
    implements HiveSinkParams<HiveSinkBatchOp> {

    public HiveSinkBatchOp() {
        this(new Params());
    }

    public HiveSinkBatchOp(Params params) {
        super(AnnotationUtils.annotatedName(HiveDB.class), params);
    }

    @Override
    public HiveSinkBatchOp sinkFrom(BatchOperator in) {
        try {
            BaseDB db = BaseDB.of(super.getParams());
            DBSinkBatchOp dbSinkBatchOp = new DBSinkBatchOp(db, getOutputTableName(), super.getParams());
            dbSinkBatchOp.linkFrom(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this;
    }
}
