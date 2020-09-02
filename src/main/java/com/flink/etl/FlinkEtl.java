package com.flink.etl;


import com.flink.config.HiveConfig;
import com.flink.config.PropertiesConstants;
import com.flink.etl.model.Rule;
import com.flink.etl.service.EventTimeBucketAssigner;
import com.flink.etl.service.FlinkEtlService;
import com.flink.config.KafkaConfig;

import com.flink.etl.service.LoadDataJob;
import com.flink.hive.HiveDB;
import com.flink.util.ExecutionEnvUtil;
import com.flink.util.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FlinkEtl {

    public static ConcurrentHashMap rules;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = ExecutionEnvUtil.prepare();
        // 每隔1分钟获取最新规则

        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(4);
        threadPool.scheduleAtFixedRate(new GetRulesJob(), 0, 1, TimeUnit.MINUTES);

        threadPool.scheduleAtFixedRate(new LoadDataJob(), 0, 1, TimeUnit.HOURS);
        HashMap<String, DataStreamSource<String>> sources = new HashMap<>();
        // Flink 读取 kafka 数据
        DataStreamSource<String> allData = null;
        for (String topic : ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SOURCE_TOPIC).split(",")) {
            if (allData == null) {
                allData = KafkaConfig.buildSource(streamEnv, topic);
            } else {
                allData.union(KafkaConfig.buildSource(streamEnv, topic));
            }
        }


        // 写 hdfs 策略
        DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy
                .create()
                .withMaxPartSize(ExecutionEnvUtil.PARAMETER_TOOL.getLong(PropertiesConstants.HDFS_FILESIZE)) // 设置每个文件的最大大小 ,默认是128M。这里设置为10M
                .withRolloverInterval(Long.MAX_VALUE) // 滚动写入新文件的时间，默认60s。这里设置为无限大
                .withInactivityInterval(10 * 1000) // 10s空闲，就滚动写入新的文件
                .build();


        String hdfsPath = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_HDFSPATH);


        while (rules == null) {
            Thread.sleep(100);
            log.info("规则尚为空,需等待");
        }

        log.info("获取规则个数： ", rules.size());

        FlinkEtlService flinkService = new FlinkEtlService();
        flinkService.setRules(rules);

        //处理加工逻辑  数据 加工
        SingleOutputStreamOperator<String> res = allData.map(flinkService).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value != null;
            }
        });

        // 处理分区
        log.info("----------------------------------------", res.getName());
        // Storage into hdfs
        EventTimeBucketAssigner eventTimeBucketAssigner = new EventTimeBucketAssigner();
        eventTimeBucketAssigner.setRules(rules);

        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<String>())
                .withBucketAssigner(eventTimeBucketAssigner)
                .withRollingPolicy(rollingPolicy)
                .build();

        res.addSink(sink);
        streamEnv.execute("Flink Streaming Java API Skeleton");
    }

    static class GetRulesJob implements Runnable {
        @Override
        public void run() {
            try {
                log.info("重新获取规则");
                rules = getRules();
            } catch (SQLException e) {
                log.error("get rules from mysql has an error {}", e.getMessage());
            }
        }


        private static ConcurrentHashMap<String, Rule> getRules() throws SQLException {
            String sql = "select * from rule";
            Connection connection = MySQLUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            ConcurrentHashMap<String, Rule> list = new ConcurrentHashMap<>();
            while (resultSet.next()) {
                Rule rule = Rule.builder()
                        .id(resultSet.getString("id"))
                        .name(resultSet.getString("name"))
                        .type(resultSet.getString("type"))
                        .code(resultSet.getString("code"))
                        .tableCode(resultSet.getString("table_code"))
                        .fields(resultSet.getString("fields"))
                        .begin(resultSet.getString("begin"))
                        .comments(resultSet.getString("comments"))
                        .columnTypes(resultSet.getString("column_types"))
                        .firstSplit(resultSet.getString("first_split"))
                        .secondSplit(resultSet.getString("second_split"))
                        .build();
                list.put(rule.getCode(), rule);

            }

            return list;
        }
    }


}
