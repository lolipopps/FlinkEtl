package com.hyt.flink.ml.feature;

import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;

public class Commic extends BaseData {
    public Commic() {
        this.schemaStr = "commercial_type_code string, website_code string, " +
                "client_code string, business_code string, one_day_pv int, " +
                "three_day_pv int, seven_day_pv int, client_ip_uv int, business_city_code_uv int, " +
                "one_stop_time bigint, three_stop_time bigint, seven_stop_time bigint, action_uv int,device_name string,one_day_housedel_uv int ," +
                "three_day_housedel_uv int,seven_day_housedel_uv int,one_day_agent_uv int,three_day_agent_uv int ,seven_day_agent_uv int,app_pv int,m_pv int,xcx_pv  int, pc_pv int,score_range int ";





        this.features = new String[]{"commercial_type_code", "website_code", "client_code", "business_code", "one_day_pv", "three_day_pv", "seven_day_pv"
                , "client_ip_uv", "business_city_code_uv", "one_stop_time",
                "three_stop_time", "seven_stop_time", "action_uv", "device_name","one_day_housedel_uv","three_day_housedel_uv","seven_day_housedel_uv","one_day_agent_uv"
        ,"three_day_agent_uv","seven_day_agent_uv","app_pv","m_pv","xcx_pv","pc_pv"};

        this.categoricalCols = new String[]{"commercial_type_code", "website_code", "client_code", "business_code","device_name"};

        this.numFeature = new String[]{"age", "capital_gain", "capital_loss", "hours_per_week",
                "workclass", "education", "marital_status", "occupation"};

        this.label = "score_range";

        getBatchData();

        getStreamData();

    }


    public void getBatchData() {
        baseSourceBatchOp = new CsvSourceBatchOp().setFilePath("src\\main\\resources\\1.csv").setFieldDelimiter("\t").setSchemaStr(schemaStr);
        getTrainData(baseSourceBatchOp);
    }


    public void getStreamData() {
        baseSourceStreamOp = new CsvSourceStreamOp().setFilePath("src\\main\\resources\\1.csv").setSchemaStr(schemaStr);
        getTrainData(baseSourceStreamOp);
    }
}
