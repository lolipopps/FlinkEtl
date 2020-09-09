package com.hyt.flink.ml;


import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.outlier.SosBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.dataproc.MultiStringIndexer;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.hyt.flink.config.HiveConfig;
import com.alibaba.alink.hive.operator.batch.HiveSourceBatchOp;
import com.hyt.flink.ml.feature.BaseData;
import com.hyt.flink.ml.feature.TableToBaseDataUtils;
import com.hyt.flink.ml.model.Model;
import com.hyt.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkSoS {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvUtil.prepareBatch();
        HiveSourceBatchOp data = new HiveSourceBatchOp()
                .setInputTableName("adult_train")
//                .setPartitions("stat_hour=2020062900")
                .setHiveVersion(HiveConfig.version)
                .setHiveConfDir(HiveConfig.hiveConfDir)
                .setDbName("ml");
        BaseData base = TableToBaseDataUtils.toBaseData(data);
        MultiStringIndexer stringindexer = new MultiStringIndexer().setSelectedCols(base.categoricalCols)
                .setStringOrderType("frequency_asc");
        BatchOperator sosData = stringindexer.fit(data).transform(data);
        VectorAssembler va = new VectorAssembler()
                .setSelectedCols(base.features)
                .setOutputCol("features");
        BatchOperator aa = va.transform(sosData);
        SosBatchOp sos = new SosBatchOp().setVectorCol("features").setPredictionCol("outlier_score").setPerplexity(3.0);
        SosBatchOp out = sos.linkFrom(aa);
        out.firstN(10).print();
    }

}
