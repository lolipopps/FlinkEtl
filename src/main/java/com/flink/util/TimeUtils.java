package com.flink.util;

import java.text.SimpleDateFormat;
import java.util.Date;



public class TimeUtils {


    public static String getHHmmss(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss SSS");
        String str = sdf.format(date);
        return str;
    }


    public static String getHHmmss(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss SSS");
        String str = sdf.format(new Date(time));
        return str;
    }


    public static String getSS(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss SSS");
        String str = sdf.format(date);
        return str;
    }


    public static String getSS(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss SSS");
        String str = sdf.format(new Date(time));
        return str;
    }

}
