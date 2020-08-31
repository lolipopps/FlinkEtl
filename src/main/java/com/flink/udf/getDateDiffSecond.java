package com.flink.udf;
import com.flink.util.DateUtil;
import org.apache.flink.table.functions.ScalarFunction;
public class getDateDiffSecond extends ScalarFunction {
    public long eval(String str1, String str2) {
        str1 = str1.toLowerCase().replace("t", " ").substring(0, 19);
        str2 = str2.toLowerCase().replace("t", " ").substring(0, 19);
        return (DateUtil.format(str2) - DateUtil.format(str1)) / 1000;
    }
}
