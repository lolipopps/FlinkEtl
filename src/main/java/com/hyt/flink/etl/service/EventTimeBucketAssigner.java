package com.hyt.flink.etl.service;

import com.hyt.flink.etl.model.Rule;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Slf4j
public class EventTimeBucketAssigner implements BucketAssigner<String, String> {
    private ObjectMapper mapper = new ObjectMapper();
    private ConcurrentHashMap<String, Rule> rules;

    @Override
    public String getBucketId(String element, Context context) {
        log.info("分区数据为： "+element);
        String ruleCode = element.split("\\$\\$")[0];
        Rule rule = rules.get(ruleCode);
        //log.info("本次的规则是：" + rule);
        Date now = new Date();
        //10分钟前的时间
        Date now_10 = new Date(now.getTime());
        //可以方便地修改日期格式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String nowTime_10 = dateFormat.format(now_10);
        // 十分钟一个分区的数据
        String partitionValue = nowTime_10.substring(0, 11) + "0";
        return "/" + rule.getTableCode() + "/pt=" + partitionValue;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }


}