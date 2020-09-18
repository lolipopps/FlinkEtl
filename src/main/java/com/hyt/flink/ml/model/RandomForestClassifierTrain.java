package com.hyt.flink.ml.model;


import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.classification.RandomForestClassifier;
import com.alibaba.alink.pipeline.classification.Softmax;
import com.hyt.flink.ml.feature.Commic;


public class RandomForestClassifierTrain {

    public static void main(String[] args) throws Exception {

//        ExecutionEnvironment streamEnv = ExecutionEnvUtil.prepareBatch();
        // 新建模型
        Model model = new Model();
        // 创建数据集
        Commic adultTrain = new Commic();
        // 算法模型
        adultTrain.trainBatchData.firstN(10).print();
        RandomForestClassifier gbdt = new RandomForestClassifier().setFeatureCols(adultTrain.getFeatures()).setCategoricalCols(adultTrain.getCategoricalCols())
                .setLabelCol(adultTrain.getLabel())
                .setPredictionDetailCol("prediction_detail");

        model.setTrainer(gbdt);
        model.setBaseData(adultTrain);
        model.train();
        model.predict(adultTrain.getTestBatchData());

//        //网格调参
//        ModelGrid modelGrid = new ModelGrid(model);
//        Double[] LEARNING_RATE = new Double[]{0.01,0.05};
//        Integer[] NUM_TREES = new Integer[]{3, 6, 9};
//        Integer[] MAX_DEPTH = new Integer[]{3, 6, 9};
//        HashMap<String, Object[]> paras = new HashMap<>();
//        paras.put("LEARNING_RATE", LEARNING_RATE);
//        paras.put("NUM_TREES", NUM_TREES);
//        paras.put("MAX_DEPTH", MAX_DEPTH);
//        modelGrid.setParamGrid(paras);
//        modelGrid.train(1);

    }
}
