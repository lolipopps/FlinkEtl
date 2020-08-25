package com.flink.ml.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.pipeline.MapTransformer;
import com.alibaba.alink.pipeline.Trainer;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.feature.FeatureHasher;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;

@Data
public abstract class BaseData implements Serializable {
    public String schemaStr;
    public String[] features;
    public String[] categoricalCols;
    public String[] numFeature;
    public String label;
    public BatchOperator trainData;
    public BatchOperator testData;
    public BaseSourceBatchOp baseSourceBatchOp;
    public FeatureHasher featureHasher;
    public ArrayList<Trainer> process = new ArrayList<Trainer>();
    public ArrayList<MapTransformer> mapProcess = new ArrayList<MapTransformer>();

    public abstract void getData();

    public void getTrainData(BatchOperator data) {
        // 训练集测试集合切分
        SplitBatchOp spliter = new SplitBatchOp().setFraction(0.9);
        spliter.linkFrom(data);
        this.trainData = spliter;
        this.testData = spliter.getSideOutput(0);
    }


}
