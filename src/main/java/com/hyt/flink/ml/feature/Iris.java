package com.hyt.flink.ml.feature;

import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;

public class Iris extends BaseData {

    public Iris() {
        this.schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
    }

    @Override
    public void getStreamData() {

    }

    @Override
    public void getBatchData() {
        CsvSourceBatchOp sourceBatchOp = new CsvSourceBatchOp().setFilePath("src\\main\\resources\\iris.csv").setSchemaStr(schemaStr);
        // 向量化
        VectorAssembler va = new VectorAssembler()
                .setSelectedCols(new String[]{"sepal_length", "sepal_width", "petal_length", "petal_width"})
                .setOutputCol("features");
        this.mapProcess.add(va);
        this.baseSourceBatchOp = sourceBatchOp;
        getTrainData(baseSourceBatchOp);
    }
}
