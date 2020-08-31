package com.flink.udf;
import org.apache.flink.table.functions.ScalarFunction;
public class getDateFormat extends ScalarFunction {
    public String eval(String str) {
        return str.toLowerCase().replace("t", " ").substring(0, 19);
    }
}
