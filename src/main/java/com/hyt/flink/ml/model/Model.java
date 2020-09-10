package com.hyt.flink.ml.model;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.GbdtPredictStreamOp;
import com.alibaba.alink.pipeline.*;
import com.hyt.flink.config.PropertiesConstants;
import com.hyt.flink.ml.feature.BaseData;
import com.hyt.flink.util.ExecutionEnvUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
        if (baseData.type == 1) {
            pipelineModel = pipeline.add(trainer).fit(baseData.getTrainBatchData());
            pipelineModel.transform(baseData.getTestBatchData()).firstN(10).print();
        } else {
            pipelineModel = pipeline.add(trainer).fit(baseData.getTrainBatchData());
            pipelineModel.transform(baseData.getTestStreamData()).print();
        }

    }


    public void save() {
        if (pipelineModel == null) {
            log.info("模型尚未训练");
        } else {
            log.info("模型保存在: " + ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_MODELPATH));
            pipelineModel.save(ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_MODELPATH));
        }
        try {
            BatchOperator.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void predict(BatchOperator baseSourceBatchOp) {
        if (pipelineModel == null) {
            pipelineModel = PipelineModel.load(ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_MODELPATH));
        }
        BatchOperator<?> res = pipelineModel.transform(baseSourceBatchOp);
        try {
            res.firstN(10).print();
            BatchOperator.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void predict(StreamOperator streamOperator) {
        System.out.println("------------------------");
        if (pipelineModel == null) {
            pipelineModel = PipelineModel.load(ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_MODELPATH));
        }
        StreamOperator<?> res = pipelineModel.transform(streamOperator);
        try {
            res.getDataStream().print();
            StreamOperator.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void predict(Table table) {
        System.out.println("------------------------");
        if (pipelineModel == null) {
            pipelineModel = PipelineModel.load(ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_MODELPATH));
        }
        Table res = pipelineModel.transform(table);
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
