package com.flink.ml.algorithm;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
/**
 * Example for GBDT.
 */
public class GBDTExample {

    public static void main(String[] args) throws Exception {

        String schema = "age bigint, workclass string, fnlwgt bigint, education string, " +
                "education_num bigint, marital_status string, occupation string, " +
                "relationship string, race string, sex string, capital_gain bigint, " +
                "capital_loss bigint, hours_per_week bigint, native_country string, label string";

        BatchOperator trainData = new CsvSourceBatchOp()
                .setFilePath("src\\main\\resources\\adult_train.csv").setSchemaStr(schema);

        BatchOperator testData = new CsvSourceBatchOp()
                .setFilePath("src\\main\\resources\\adult_test.csv").setSchemaStr(schema);

        GbdtClassifier gbdt = new GbdtClassifier()
                // 连续字段
                .setFeatureCols(new String[]{"age", "capital_gain", "capital_loss", "hours_per_week",
                        "workclass", "education", "marital_status", "occupation"})
                // 枚举字段
                .setCategoricalCols(new String[]{"workclass", "education", "marital_status", "occupation"})
                // 标签字段
                .setLabelCol("label")
                .setNumTrees(20)
                .setPredictionCol("prediction_result");

        gbdt.fit(trainData).transform(testData.firstN(10)).print();
    }
}