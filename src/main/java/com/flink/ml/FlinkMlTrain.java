package com.flink.ml;


import com.alibaba.alink.operator.batch.sink.TextSinkBatchOp;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.tuning.ParamGrid;
import com.flink.ml.feature.AdultTrain;
import com.flink.ml.feature.AvazuSmall;
import com.flink.ml.feature.Iris;
import com.flink.ml.model.Model;
import com.flink.ml.model.ModelGrid;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;


public class FlinkMlTrain {

    public static void main(String[] args) throws Exception {

//        ExecutionEnvironment streamEnv = ExecutionEnvUtil.prepareBatch();

        // 新建模型
        Model model = new Model();
        // 创建数据集
        AdultTrain adultTrain = new AdultTrain();
        // 算法模型


        GbdtClassifier gbdt = new GbdtClassifier().setFeatureCols(adultTrain.getFeatures()).setCategoricalCols(adultTrain.getCategoricalCols())
                .setLabelCol(adultTrain.getLabel())
                .setNumTrees(10).setPredictionCol("prediction_result")
                .setPredictionDetailCol("prediction_detail");
        model.setTrainer(gbdt);
        model.setBaseData(adultTrain);
//        model.train();
//        //网格调参
        ModelGrid modelGrid = new ModelGrid(model);

        Double[] LEARNING_RATE = new Double[]{0.01,0.05};
 //       Integer[] NUM_TREES = new Integer[]{3, 6, 9};
//        Integer[] MAX_DEPTH = new Integer[]{3, 6, 9};
//
        HashMap<String, Object[]> paras = new HashMap<>();
        paras.put("LEARNING_RATE", LEARNING_RATE);
//        paras.put("NUM_TREES", NUM_TREES);
//        paras.put("MAX_DEPTH", MAX_DEPTH);
        modelGrid.setParamGrid(paras);

        modelGrid.train(1);
//        streamEnv.execute("模型任务");

    }
}
