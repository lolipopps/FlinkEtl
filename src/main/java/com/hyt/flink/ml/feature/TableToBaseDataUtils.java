package com.hyt.flink.ml.feature;

import com.alibaba.alink.hive.operator.batch.HiveSourceBatchOp;
import com.alibaba.alink.hive.operator.stream.HiveSourceStreamOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;

import java.util.ArrayList;

public class TableToBaseDataUtils {
    public static BaseData toBaseData(HiveSourceBatchOp data) {
        BaseData baseData = new BaseData();
        TableSchema tableS = data.getSchema();
        ArrayList<String> columns = new ArrayList<>();
        ArrayList<String> categoricalCols = new ArrayList<>();
        ArrayList<String> numFeature = new ArrayList<>();
        StringBuilder schemaStr = new StringBuilder();
        StringBuilder colNames = new StringBuilder();
        for (TableColumn col : tableS.getTableColumns()) {
            if (col.getName().toLowerCase().equals("class") || col.getName().toLowerCase().equals("label")) {
                baseData.setLabel(col.getName());
                continue;
            }
            columns.add(col.getName());
            schemaStr.append(col.getName() + " " + col.getType().toString().toLowerCase() + ",");
            colNames.append(col.getName() + ",");
            if (col.getType().toString().toLowerCase().equals("string")) {
                categoricalCols.add(col.getName());
            } else {
                numFeature.add(col.getName());
            }
        }
        baseData.setNumFeature(numFeature.toArray(new String[numFeature.size()]));
        baseData.setSchemaStr(schemaStr.toString().substring(0, schemaStr.length() - 1));
        baseData.setCategoricalCols(categoricalCols.toArray(new String[categoricalCols.size()]));
        baseData.setFeatures(columns.toArray(new String[columns.size()]));
        baseData.setColNames(colNames.toString().substring(0, colNames.length() - 1));
        baseData.getTrainData(data.select("*"));
        baseData.getTrainData(data.select("*"));
        return baseData;
    }
    public static BaseData toBaseData(HiveSourceStreamOp data) {
        BaseData baseData = new BaseData();
        TableSchema tableS = data.getSchema();
        ArrayList<String> columns = new ArrayList<>();
        ArrayList<String> categoricalCols = new ArrayList<>();
        ArrayList<String> numFeature = new ArrayList<>();
        StringBuilder schemaStr = new StringBuilder();
        StringBuilder colNames = new StringBuilder();
        for (TableColumn col : tableS.getTableColumns()) {
            if (col.getName().toLowerCase().equals("class") || col.getName().toLowerCase().equals("label")) {
                baseData.setLabel(col.getName());
                continue;
            }
            columns.add(col.getName());
            schemaStr.append(col.getName() + " " + col.getType().toString().toLowerCase() + ",");
            colNames.append(col.getName() + ",");
            if (col.getType().toString().toLowerCase().equals("string")) {
                categoricalCols.add(col.getName());
            } else {
                numFeature.add(col.getName());
            }
        }
        baseData.setNumFeature(numFeature.toArray(new String[numFeature.size()]));
        baseData.setSchemaStr(schemaStr.toString().substring(0, schemaStr.length() - 1));
        baseData.setCategoricalCols(categoricalCols.toArray(new String[categoricalCols.size()]));
        baseData.setFeatures(columns.toArray(new String[columns.size()]));
        baseData.setColNames(colNames.toString().substring(0, colNames.length() - 1));
        baseData.getTrainData(data.select("*"));
        return baseData;
    }

}
