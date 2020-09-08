package com.hyt.flink.ml.model;

import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.pipeline.PipelineModel;


import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.Pipeline;
import com.hyt.flink.config.PropertiesConstants;
import com.hyt.flink.util.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamModel {
    // 存模型
    Pipeline pipeline;
    PipelineModel pipelineModel;
    BaseSourceBatchOp baseSourceBatchOp;

    public StreamModel(Pipeline pipeline, BaseSourceBatchOp baseSourceBatchOp) {
        this.pipeline = pipeline;
        this.baseSourceBatchOp = baseSourceBatchOp;
    }
    public void fit() {
        pipelineModel = pipeline.fit(baseSourceBatchOp);
    }

    public void save() {
        if (pipelineModel == null) {
            log.info("模型尚未训练");
        } else {
            log.info("模型保存在: " + ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_MODELPATH));
            pipelineModel.save(ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_MODELPATH));
        }
    }

    public void predict(BaseSourceBatchOp baseSourceBatchOp) {
        if (pipelineModel == null) {
            pipelineModel = PipelineModel.load(ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_MODELPATH));
            BatchOperator<?> res = pipelineModel.transform(baseSourceBatchOp);
            res.firstN(10);
        }
    }


}
