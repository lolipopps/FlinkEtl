package com.hyt.flink.util;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Author: hyt
 * @License: (C) Copyright 2020-2020, xxx Corporation Limited.
 * @Contact: xxx@xxx.com
 * @Version: 1.0
 * @Description:
 */
public class GsonUtil {
    private final static Gson gson = new Gson();

    public static <T> T fromJson(String value, Class<T> type) {
        return gson.fromJson(value, type);
    }

    public static String toJson(Object value) {
        return gson.toJson(value);
    }

    public static byte[] toJSONBytes(Object value) {
        return gson.toJson(value).getBytes(Charset.forName("UTF-8"));
    }

    public static HashMap<String, String> getJsonKey(String str) {
        JSONObject jsonObject = JSONObject.parseObject(str);
        Set<String> keys = jsonObject.keySet();
        HashMap<String, String> res = new HashMap<>();
        for (String key : keys) {
            res.put(key, jsonObject.get(key).getClass().getSimpleName().toLowerCase().replace("integer", "int"));
        }
        return res;
    }



    public static ArrayList<String> getCodeAndType(String str) {
        HashMap<String, String> res = getJsonKey(str);
        StringBuilder colCode = new StringBuilder();
        StringBuilder colType = new StringBuilder();
        for (Map.Entry<String, String> re : res.entrySet()) {
            if (colCode.length() == 0) {
                colCode.append(re.getKey());
                colType.append(re.getValue());
            } else {
                colCode.append("," + re.getKey());
                colType.append("," + re.getValue());
            }
        }
        ArrayList<String> resList = new ArrayList<String>();
        resList.add(colCode.toString());
        resList.add(colType.toString());
        return resList;
    }
}
