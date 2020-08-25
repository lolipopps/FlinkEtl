package com.flink.feature;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

public class FlinkFeatureService extends RichMapFunction<String, BaseSourceTable> {
    @Override
    public BaseSourceTable map(String source) throws Exception {
        source = source.toLowerCase();
        JSONObject jsonObject = JSONObject.parseObject(source);
        Long eventTime = jsonObject.getLong("event_time");
        String logType = jsonObject.getString("log_type");
        BaseSourceTable baseSourceTable = new BaseSourceTable();
        baseSourceTable.setContent(source);
        baseSourceTable.setEventTime(eventTime);
        baseSourceTable.setLogType(logType);
        return baseSourceTable;
    }
}
