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
public class FlinkEtlService extends RichMapFunction<String, String> {
    private ConcurrentHashMap<String, Rule> rules;

    @Override
    public String map(String source) {
          log.info("原始数据为  " + source);
        StringBuilder res = new StringBuilder();
        Rule value = null;
        // 这一点可以优化要是能快速确认源
        for (Map.Entry<String, Rule> key : rules.entrySet()) {
            //  匹配规则
            if (source.contains(key.getKey()) || (key.getValue().getBegin() != null && source.contains(key.getValue().getBegin()))) {
                value = key.getValue();
                String[] fields = value.getFields().split(",");
                // 传过来的数据是json格式的
                if (value.getFirstSplit().equalsIgnoreCase("json")) {
                    source = source.toLowerCase();
                    JSONObject jsonObject = JSONObject.parseObject(source);
                    for (String fieldCode : fields) {
                        String colValue = jsonObject.get(fieldCode) == null ? "" : jsonObject.get(fieldCode).toString();
                        res.append(colValue + "$$");
                    }

                    // 有分隔符的情况
                } else if (value.getFirstSplit() != null || !value.getFirstSplit().equals("")) {

                    // 切出需要的数据
                    String begin = key.getValue().getBegin();
                    if (begin != null && !begin.equals("")) {
                        source = source.split(begin)[1];
                    }


                    String first = value.getFirstSplit().replace("\\\\", "\\");
                    // 只有一个分隔符 中间没有分隔符的
                    String[] fieldContents = source.split(first);
                    if (value.getSecondSplit() == null || value.getSecondSplit().equals("")) {// 严格规定字段顺序 不然会错位
                        // 直接用字符串替换
                        for (String fieldContent : fieldContents) {
                            res.append(fieldContent + "$$");
                        }


                    } else {   // 两个分隔符的情况

                        String second = value.getFirstSplit().replace("\\\\", "\\");
                        for (String fieldContent : fieldContents) {
                            String[] contents = fieldContent.split(second);
                            if (contents.length < 2) {
                                res.append("$$");
                            } else {
                                res.append(contents[1] + "$$");
                            }
                        }
                    }
                }
                break;
            }

        }
        String result;

        if (value == null) {
            return null;
        } else {
            if (res.toString().endsWith("$$")) {
                result = value.getCode() + "$$" + res.substring(0, res.length() - 2).toString();
            } else {
                result = value.getCode() + "$$" + res.toString();
            }
        }
        //   log.info("结果数据  " + result);
        return result;
    }
}
