package com.flink.ml.model;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.sink.TextSinkBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.pipeline.tuning.*;
import com.flink.ml.feature.BaseData;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.Table;

import java.util.HashMap;
import java.util.Map;


@Slf4j
@Data
public class ModelGrid {


    // 参数
    public ParamGrid paramGrid = new ParamGrid();


    public Model model;

    public GridSearchCV gridSearchCV;

    public ModelGrid(Model model) {
        this.model = model;
    }

    public void getMultiClassGridSearchCV() {

        TuningEvaluator tuningEvaluator = new MultiClassClassificationTuningEvaluator().setPredictionCol("prediction")
                .setLabelCol(model.getBaseData().getLabel())
                .setPredictionDetailCol("prediction_detail").setTuningMultiClassMetric("ACCURACY");
        gridSearchCV = new GridSearchCV().setEstimator(model.getTrainer()).setParamGrid(paramGrid).setNumFolds(2);
//        printClassMetrics(metrics);

    }


    public void getBinClassGridSearchCV() {

        BinaryClassificationTuningEvaluator binaryClassificationTuningEvaluator = new BinaryClassificationTuningEvaluator()
                .setLabelCol(model.getBaseData().getLabel())
                .setPredictionDetailCol("prediction_detail")
                .setTuningBinaryClassMetric("AUC");
        gridSearchCV = new GridSearchCV().setEstimator(model.getTrainer())
                .setTuningEvaluator(binaryClassificationTuningEvaluator)
                .setParamGrid(paramGrid).setNumFolds(5);

    }


    public void getRegressionGridSearchCV() {
        RegressionMetrics metrics = new EvalRegressionBatchOp()
                .setLabelCol(model.getBaseData().getLabel()).setPredictionCol("prediction").collectMetrics();
        gridSearchCV = new GridSearchCV().setEstimator(model.getTrainer()).setParamGrid(paramGrid).setNumFolds(2);


    }

    // 参数
    public void setParamGrid(HashMap<String, Object[]> paras) {
        try {
            for (Map.Entry<String, Object[]> para : paras.entrySet()) {
                paramGrid.addGrid(model.getTrainer(), para.getKey(), para.getValue());
            }
            log.info(paramGrid.toString());
        } catch (Exception e) {
            log.info("paramGrid error", e.getMessage());
        }
    }

    public void printClassMetrics(MultiClassMetrics metrics) {
        log.info("LogLoss: {}", metrics.getLogLoss());
        log.info("Macro Precision: {}", metrics.getMacroPrecision());
        log.info("Micro Recall: {}", metrics.getMicroRecall());
        log.info("Weighted Sensitivity: {}", metrics.getWeightedSensitivity());

    }

    public void printRegressionMetrics(RegressionMetrics metrics) {
        log.info("Total Samples Number: {}", metrics.getCount());
        log.info("SSE: {}", metrics.getSse());
        log.info("SAE: {}", metrics.getSae());
        log.info("RMSE: {}", metrics.getRmse());
        log.info("R2: {}", metrics.getR2());
    }

    public void train(int type) throws Exception {
        if (type == 1) {
            getBinClassGridSearchCV();
        } else if (type == 2) {
            getMultiClassGridSearchCV();
        } else {
            getRegressionGridSearchCV();
        }
        GridSearchCVModel modl = gridSearchCV.fit(model.baseData.getTrainBatchData());

        BatchOperator inOp = modl.transform(model.baseData.getTestBatchData());

        BinaryClassMetrics metrics = new EvalBinaryClassBatchOp().setLabelCol("label").setPredictionDetailCol("prediction_detail" +
                "").linkFrom(inOp).collectMetrics();
        System.out.println("AUC: " + metrics.getAuc());
        System.out.println("KS: " + metrics.getKs());
        System.out.println("PRC: " + metrics.getPrc());
        System.out.println("Accuracy: " + metrics.getAccuracy());
        System.out.println("Macro Precision: " + metrics.getMacroPrecision());
        System.out.println("Micro Recall: " + metrics.getMicroRecall());
        System.out.println("Weighted Sensitivity: " + metrics.getWeightedSensitivity());

    }


}
