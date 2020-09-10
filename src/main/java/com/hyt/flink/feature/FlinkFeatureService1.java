package com.hyt.flink.feature;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class FlinkFeatureService1 extends RichMapFunction<String, BaseSource> {
    @Override
    public BaseSource map(String source) throws Exception {

        BaseSource baseSource = new BaseSource();
        baseSource.setContent(source);
        baseSource.setEventTime(new Date().getTime());
        baseSource.setLogType("1");
        baseSource.setRowTime(new Date().getTime());
//        System.out.println(baseSource.toString());
        return baseSource;
    }
}
