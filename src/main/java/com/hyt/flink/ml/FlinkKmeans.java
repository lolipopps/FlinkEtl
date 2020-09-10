package com.hyt.flink.ml;


import com.alibaba.alink.hive.operator.batch.HiveSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.hyt.flink.config.HiveConfig;
import com.hyt.flink.ml.feature.BaseData;
import com.hyt.flink.ml.feature.TableToBaseDataUtils;
import com.hyt.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkKmeans {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvUtil.prepareBatch();
//        HiveSourceBatchOp data = new HiveSourceBatchOp()
//                .setInputTableName("audit_linuxserver_abnormalprogres_2020062900")
////                .setPartitions("stat_hour=2020062900")
//                .setHiveVersion(HiveConfig.version)
//                .setHiveConfDir(HiveConfig.hiveConfDir)
//                .setDbName(HiveConfig.dbName);
//        System.out.println(data.getColNames());

        HiveSourceBatchOp data = new HiveSourceBatchOp()
                .setInputTableName("adult_train")
//                .setPartitions("stat_hour=2020062900")
                .setHiveVersion(HiveConfig.version)
                .setHiveConfDir(HiveConfig.hiveConfDir)
                .setDbName("ml");

        BaseData base = TableToBaseDataUtils.toBaseData(data);



        VectorAssembler va = new VectorAssembler()
                .setSelectedCols(base.numFeature)
                .setOutputCol("features");

        KMeans kMeans = new KMeans().setVectorCol("features").setK(3).setReservedCols(base.categoricalCols)
                .setPredictionCol("prediction_result")
                .setPredictionDetailCol("prediction_detail")
                .setReservedCols("category")
                .setMaxIter(100);
        Pipeline pipeline = new Pipeline().add(va).add(kMeans);
        pipeline.fit(data).transform(data).firstN(10).print();

    }

}
