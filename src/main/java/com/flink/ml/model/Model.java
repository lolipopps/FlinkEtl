package com.flink.ml.model;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.pipeline.*;
import com.flink.config.PropertiesConstants;
import com.flink.ml.feature.BaseData;
import com.flink.util.ExecutionEnvUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class Model {
    // 存模型
    Pipeline pipeline = new Pipeline();
    PipelineModel pipelineModel;

    // 模型名称
    Trainer trainer;
    // 模型数据
    BaseData baseData;

    public void train() throws Exception {

        // 进一步模型
        for (Trainer trainer : baseData.getProcess()) {
            pipeline.add(trainer);
        }


        // 加载特征工程处理步骤
        for (MapTransformer mapTransformer : baseData.getMapProcess()) {
            pipeline.add(mapTransformer);
        }

        // 加载特征头部
        if (baseData.getFeatureHasher() != null) {
            pipeline.add(baseData.getFeatureHasher());
        }


        // 训练模型
        pipelineModel = pipeline.add(trainer).fit(baseData.getTrainData());

        pipelineModel.transform(baseData.getTestData()).firstN(10).print();

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
