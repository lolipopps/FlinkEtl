package com.flink.udf.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;


public class SplitTable extends TableFunction<Tuple2<String, Integer>> {

    private String separator;


    public SplitTable(String separator) {
        this.separator = separator;
    }



    public void eval(String str) {
        if (str.indexOf("flink") < 0) {
            for (String s : str.split(separator)) {
                collect(new Tuple2<String, Integer>(s, s.length()));
            }
        }
    }
}