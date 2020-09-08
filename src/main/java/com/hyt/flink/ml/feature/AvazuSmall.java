package com.hyt.flink.ml.feature;

import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;

public class AvazuSmall extends BaseData {

    public AvazuSmall() {
        this.schemaStr = "id string, click string, dt string, C1 string, banner_pos int, site_id string, site_domain string, "
                + "site_category string, app_id string, app_domain string, app_category string, device_id string, "
                + "device_ip string, device_model string, device_type string, device_conn_type string, C14 int, C15 int, "
                + "C16 int, C17 int, C18 int, C19 int, C20 int, C21 int";
    }



    @Override
    public void getStreamData() {

    }

    @Override
    public void getBatchData() {
        CsvSourceBatchOp trainBatchData = new CsvSourceBatchOp()
                .setFilePath("src\\main\\resources\\avazu-small.csv")
                .setSchemaStr(schemaStr);
        trainBatchData.firstN(10);

        String[] selectedColNames = new String[]{
                "C1", "banner_pos", "site_category", "app_domain",
                "app_category", "device_type", "device_conn_type",
                "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21",
                "site_id", "site_domain", "device_id", "device_model"};
        String[] categoryColNames = new String[]{
                "C1", "banner_pos", "site_category", "app_domain",
                "app_category", "device_type", "device_conn_type",
                "site_id", "site_domain", "device_id", "device_model"};
        String[] numericalColNames = new String[]{
                "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21"};
        // result column name of feature engineering
        String vecColName = "vec";
        int numHashFeatures = 30000;
        featureHasher.setSelectedCols(selectedColNames)
                .setCategoricalCols(categoryColNames)
                .setOutputCol(vecColName)
                .setNumFeatures(numHashFeatures);
        StandardScaler standardScaler = new StandardScaler()
                .setSelectedCols(numericalColNames);

        process.add(standardScaler);
    }
}
