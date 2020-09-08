package com.hyt.flink.feature;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class FlinkFeatureService extends RichMapFunction<String, BaseSource> {
    @Override
    public BaseSource map(String source) throws Exception {
        source = source.toLowerCase();
        JSONObject jsonObject = JSONObject.parseObject(source);
        Object eventTimeTemp = jsonObject.get("event_time");
        String tt = String.valueOf(eventTimeTemp);
        Long eventTime = 0L;
        if(NumberUtils.isDigits(tt)) {
            eventTime = jsonObject.getLong("event_time");
        }else{
            tt = tt.toLowerCase().substring(0,19).replace("t"," ");
            DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            eventTime = df2.parse(tt).getTime();
        }
        String logType = jsonObject.getString("log_type");
        BaseSource baseSource = new BaseSource();
        baseSource.setContent(source);
        baseSource.setEventTime(eventTime);
        baseSource.setLogType(logType);
        baseSource.setRowTime(new Date().getTime());
//        System.out.println(baseSource.toString());
        return baseSource;
    }
}
