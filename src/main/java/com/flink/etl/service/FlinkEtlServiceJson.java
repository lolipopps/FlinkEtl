package com.flink.etl.service;

import com.alibaba.fastjson.JSONObject;
import com.flink.etl.model.Rule;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Slf4j
public class FlinkEtlServiceJson extends RichMapFunction<String, String> {

    private ConcurrentHashMap<String, Rule> rules;

    @Override
    public String map(String source) {
        //   log.info("原始数据为  " + source);
        StringBuilder res = new StringBuilder();
        Rule value = null;
        source = source.toLowerCase();
        // 这一点可以优化要是能快速确认源
        JSONObject jsonObject = JSONObject.parseObject(source);
        value = rules.get(jsonObject.get("log_type"));

        String[] fields = value.getFields().split(",");
        // 传过来的数据是json格式的
        for (String fieldCode : fields) {
            String colValue = jsonObject.get(fieldCode) == null ? "" : jsonObject.get(fieldCode).toString();
            res.append(colValue + "$$");
        }
        return res.toString();
    }
}
