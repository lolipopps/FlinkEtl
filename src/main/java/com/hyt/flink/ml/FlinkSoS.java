package com.hyt.flink.ml;


import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.outlier.SosBatchOp;
import com.alibaba.alink.operator.batch.sql.LeftOuterJoinBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.dataproc.MultiStringIndexer;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScaler;
import com.hyt.flink.config.HiveConfig;
import com.alibaba.alink.hive.operator.batch.HiveSourceBatchOp;
import com.hyt.flink.ml.feature.BaseData;
import com.hyt.flink.ml.feature.TableToBaseDataUtils;
import com.hyt.flink.ml.model.Model;
import com.hyt.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;
import org.apache.flink.table.api.Table;

import static org.apache.flink.table.api.Expressions.$;

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
        BatchOperator train = base.trainBatchData.sample(1);
        AppendIdBatchOp indexData = new AppendIdBatchOp().setIdCol("index").linkFrom(train);

        MultiStringIndexer stringindexer = new MultiStringIndexer().setSelectedCols(base.categoricalCols)
                .setStringOrderType("frequency_asc");
        BatchOperator sosData = stringindexer.fit(train).transform(train);

        VectorAssembler va = new VectorAssembler()
                .setSelectedCols(base.features)
                .setOutputCol("features");

        BatchOperator aa = va.transform(sosData).select("features");
        VectorMinMaxScaler vectorMinMaxScaler = new VectorMinMaxScaler().setSelectedCol("features");
        BatchOperator res = vectorMinMaxScaler.fit(aa).transform(aa);
        SosBatchOp sos = new SosBatchOp().setVectorCol("features").setPredictionCol("outlier_score").setPerplexity(3.0);
        SosBatchOp out = sos.linkFrom(res);
        AppendIdBatchOp indexDataOut = new AppendIdBatchOp().setIdCol("indexs").linkFrom(out);
        indexDataOut.firstN(10).print();
        indexData.firstN(10).print();
        BatchOperator topk = indexDataOut.orderBy("outlier_score", 100, false);
        Table topkTable = topk.getOutputTable();
        Table indexDataTable = indexData.getOutputTable();
        topkTable.join(indexDataTable).where($("indexs").isEqual($("index"))).select(base.colNames+",outlier_score,indexs").execute().print();

    }

}
