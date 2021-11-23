package com.flink.udf;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class ScalarFunctionTemplate extends ScalarFunction {


    public String eval(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(dateStr);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.format(date);
    }


    public String eval(String dateStr, int num) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        Date date = sdf.parse(dateStr);
        calendar.setTime(date);
        int day = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, day - num);
        String lastDay = sdf.format(calendar.getTime());
        return lastDay;
    }

    private int sleepTime;


    @Override
    public void open(FunctionContext context) {
        sleepTime = Integer.valueOf(context.getJobParameter("sleepTime", "1"));
    }


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        Configuration conf = new Configuration();
        conf.setString("sleepTime", "2");
        env.getConfig().setGlobalJobParameters(conf);

        List<Row> data = new ArrayList<>();
        data.add(Row.of("intsmaze", "2019-07-28 12:00:00", ".../intsmaze/"));
        data.add(Row.of("Flink", "2019-07-25 12:00:00", ".../intsmaze/"));
        DataSet<Row> orderRegister = env.fromCollection(data);
        tableEnv.registerDataSet("testSUDF", orderRegister, "user,visit_time,url");
        tableEnv.registerFunction("custom_Date", new ScalarFunctionTemplate());
        Table sqlResult = tableEnv.sqlQuery("SELECT user, custom_Date( visit_time ),custom_Date( visit_time ,2) FROM testSUDF");
        DataSet<Row> result = tableEnv.toDataSet(sqlResult, Row.class);
        result.print("result");
        env.execute();
    }

}
