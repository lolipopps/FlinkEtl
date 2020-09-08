package com.hyt.flink.ml.feature;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;

public class AdultTrain extends BaseData {
    public AdultTrain() {
         this.schemaStr   = "age bigint, workclass string, fnlwgt bigint, education string, " +
                "education_num bigint, marital_status string, occupation string, " +
                "relationship string, race string, sex string, capital_gain bigint, " +
                "capital_loss bigint, hours_per_week bigint, native_country string, label string";

        this.features = new String[]{"age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation"
                , "relationship", "race", "sex",
                "capital_gain", "capital_loss", "hours_per_week", "native_country"};

        this.categoricalCols = new String[]{"workclass", "education", "marital_status", "occupation"};

        this.numFeature = new String[]{"age", "capital_gain", "capital_loss", "hours_per_week",
                "workclass", "education", "marital_status", "occupation"};

        this.label = "label";
        getBatchData();
    }


    @Override
    public void getBatchData() {
        baseSourceBatchOp = new CsvSourceBatchOp().setFilePath("hdfs://hadoop:9000/ml/adult_train.csv").setSchemaStr(schemaStr);
        getTrainData(baseSourceBatchOp);
    }



    @Override
    public void getStreamData() {
        baseSourceStreamOp = new CsvSourceStreamOp().setFilePath("hdfs://hadoop:9000/ml/adult_train.csv").setSchemaStr(schemaStr);
        getTrainData(baseSourceStreamOp);
    }
}
