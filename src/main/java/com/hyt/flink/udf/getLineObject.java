package com.hyt.flink.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;

public class getLineObject extends ScalarFunction {

    public String eval(String str, int index, String split) {
        String[] res = str.split(split);
        if (res.length < index) {
            return null;
        } else {
            return res[index - 1];

        }
    }


}

