package com.hyt.flink.ml.feature;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.SplitStreamOp;
import com.alibaba.alink.operator.stream.source.BaseSourceStreamOp;
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
    public BatchOperator trainBatchData;
    public BatchOperator testBatchData;
    public BaseSourceBatchOp baseSourceBatchOp;

    public StreamOperator trainStreamData;
    public StreamOperator testStreamData;
    public BaseSourceStreamOp baseSourceStreamOp;



    public FeatureHasher featureHasher;
    public ArrayList<Trainer> process = new ArrayList<Trainer>();
    public ArrayList<MapTransformer> mapProcess = new ArrayList<MapTransformer>();

    public abstract void getStreamData();

    public abstract void getBatchData();


    public void getTrainData(BaseSourceStreamOp data) {
        // 训练集测试集合切分
        SplitStreamOp streamSpliter = new SplitStreamOp().setFraction(0.9);
        streamSpliter.linkFrom(data);
        this.trainStreamData = streamSpliter;
        this.testStreamData = streamSpliter.getSideOutput(0);
    }

    public void getTrainData(BaseSourceBatchOp data) {
        // 训练集测试集合切分
        SplitBatchOp batchSpliter = new SplitBatchOp().setFraction(0.9);
        batchSpliter.linkFrom(data);
        this.trainBatchData = batchSpliter;
        this.testBatchData = batchSpliter.getSideOutput(0);
    }


}
