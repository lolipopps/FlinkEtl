package com.hyt.flink.ml;


import com.alibaba.alink.hive.operator.batch.HiveSourceBatchOp;
import com.alibaba.alink.hive.operator.stream.HiveSourceStreamOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.hyt.flink.config.HiveConfig;
import com.hyt.flink.config.KafkaConfig;
import com.hyt.flink.config.PropertiesConstants;
import com.hyt.flink.feature.StreamTableFeature;
import com.hyt.flink.ml.feature.AdultTrain;
import com.hyt.flink.ml.feature.BaseData;
import com.hyt.flink.ml.feature.TableToBaseDataUtils;
import com.hyt.flink.ml.model.Model;
import com.hyt.flink.udf.getDateDiffSecond;
import com.hyt.flink.udf.getDateFormat;
import com.hyt.flink.udf.getDateMin;
import com.hyt.flink.udf.getJsonObject;
import com.hyt.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkGBDTStream {

    public static void main(String[] args) throws Exception {
        // 新建模型
        Model model = new Model();
        HiveSourceStreamOp data = new HiveSourceStreamOp()
                .setInputTableName("adult_train")
//                .setPartitions("stat_hour=2020062900")
                .setHiveVersion(HiveConfig.version)
                .setHiveConfDir(HiveConfig.hiveConfDir)
                .setDbName("ml");
        BaseData base = TableToBaseDataUtils.toBaseData(data);
        model.predict(base.getTestStreamData());
    }
}
