package com.hyt.flink.ml;


import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.hyt.flink.ml.feature.AdultTrain;
import com.hyt.flink.ml.model.Model;
import com.hyt.flink.ml.model.ModelGrid;

import java.util.HashMap;


public class FlinkGBDT {

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
        Integer[] NUM_TREES = new Integer[]{3, 6, 9};
        Integer[] MAX_DEPTH = new Integer[]{3, 6, 9};
        HashMap<String, Object[]> paras = new HashMap<>();
        paras.put("LEARNING_RATE", LEARNING_RATE);
        paras.put("NUM_TREES", NUM_TREES);
        paras.put("MAX_DEPTH", MAX_DEPTH);
        modelGrid.setParamGrid(paras);
        modelGrid.train(1);

    }
}
