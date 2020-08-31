package com.flink.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;

public class getJsonObject extends ScalarFunction {

    public String eval(String str, String name) {
        JSONObject jsonObject = JSONObject.parseObject(str);
        String res = String.valueOf(jsonObject.get(name));
        return res;
    }


}

