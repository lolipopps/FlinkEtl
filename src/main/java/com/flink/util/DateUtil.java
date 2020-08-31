package com.flink.util;


import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**

 * name：Huyt
 */
public class DateUtil {

    public static DateTimeFormatter YYYY_MM_DD = DateTimeFormat.forPattern("yyyy-MM-dd");
    public static DateTimeFormatter YYYYMMDD = DateTimeFormat.forPattern("yyyyMMdd");
    public static DateTimeFormatter YYYYMMDDHHMMSS = DateTimeFormat.forPattern("yyyyMMddHHmmss");
    public static DateTimeFormatter YYYY_MM_DD_HH_MM_SS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    public static DateTimeFormatter YYYY_MM_DD_HH_MM_SS_0 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.0");
    public static DateTimeFormatter YYYY_MM_DD_HH_MM = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");
    public static DateTimeFormatter YYYYMMDD_HH_MM_SS = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");


    public static String format(Date date, DateTimeFormatter dateFormatter) {
        DateTime dateTime = new DateTime(date);
        return dateTime.toString(dateFormatter);
    }

    public static String format(Date date, DateTimeZone timeZone, DateTimeFormatter dateFormatter) {
        DateTime dateTime = new DateTime(date, timeZone);
        return dateTime.toString(dateFormatter);
    }

    public static String format(long timeStamp, DateTimeFormatter dateFormatter) {
        return format(timeStamp, "Asia/Shanghai", dateFormatter);
    }

    public static String format(long timeStamp, String timeZoneId, DateTimeFormatter dateFormatter) {
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneId);
        DateTime dateTime = new DateTime(timeStamp, timeZone);
        return dateTime.toString(dateFormatter);
    }

    /**
     * 格式化日期
     *
     * @param time
     * @param dateFormatter
     * @return
     */
    public static long format(String time, DateTimeFormatter dateFormatter) {
        if (YYYY_MM_DD_HH_MM_SS_0.equals(dateFormatter)) {
            return YYYY_MM_DD_HH_MM_SS_0.parseMillis(time);
        } else if (YYYY_MM_DD_HH_MM_SS.equals(dateFormatter)) {
            return YYYY_MM_DD_HH_MM_SS.parseMillis(time);
        } else if (YYYY_MM_DD_HH_MM.equals(dateFormatter)) {
            return YYYY_MM_DD_HH_MM.parseMillis(time);
        } else if (YYYY_MM_DD.equals(dateFormatter)) {
            return YYYY_MM_DD.parseMillis(time);
        }
        return YYYY_MM_DD_HH_MM_SS.parseMillis(time);
    }

    public static long format(Date date) {
        return YYYY_MM_DD_HH_MM_SS.parseMillis(format(date, YYYY_MM_DD_HH_MM_SS));
    }

    public static long format(String date) {
        Date temp = toDate(date, YYYY_MM_DD_HH_MM_SS);
        return YYYY_MM_DD_HH_MM_SS.parseMillis(format(temp, YYYY_MM_DD_HH_MM_SS));
    }
    /**
     * 判断时间是否有效
     *
     * @param value
     * @param formatter
     * @return
     */
    public static Boolean isValidDate(String value, DateTimeFormatter formatter) {
        try {
            formatter.parseDateTime(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 根据传入值的时间字符串和格式，输出Date类型
     *
     * @param value
     * @param formatter
     * @return
     */
    public static Date toDate(String value, DateTimeFormatter formatter) {
        return formatter.parseDateTime(value).toDate();
    }


    /**
     * 获取当天的开始时间的字符串
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfDay(Date date, DateTimeFormatter formatter) {
        return new DateTime(date).withTimeAtStartOfDay().toString(formatter);
    }

    /**
     * 获取当天的开始时间的字符串
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfDay(DateTime date, DateTimeFormatter formatter) {
        return date.withTimeAtStartOfDay().toString(formatter);
    }

    /**
     * 获取当天的结束时间的字符串
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfDay(Date date, DateTimeFormatter formatter) {
        return new DateTime(date).withTimeAtStartOfDay().plusDays(1).minusSeconds(1).toString(formatter);
    }

    /**
     * 获取当天的结束时间的字符串
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfDay(DateTime date, DateTimeFormatter formatter) {
        return date.withTimeAtStartOfDay().plusDays(1).minusSeconds(1).toString(formatter);
    }


    /**
     * 获取当天的结束时间的字符串
     *
     * @return 一小时前
     */
    public static String getBeforeHour() {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHH");
        Calendar c=Calendar.getInstance();
        c.add(Calendar.HOUR_OF_DAY, -1);//1小时前
        return sdf.format(c.getTime());

    }


    /**
     * 获取Now的开始时间的字符串
     * 格式默认YYYYMMDDHHMMSS
     *
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfNow() {
        return DateTime.now().withTimeAtStartOfDay().toString(YYYYMMDDHHMMSS);
    }

    /**
     * 获取Now的结束时间的字符串
     * 格式默认YYYYMMDDHHMMSS
     *
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfNow() {
        return DateTime.now().withTimeAtStartOfDay().plusDays(1).minusSeconds(1).toString(YYYYMMDDHHMMSS);
    }

    public static void main(String[] args){
        //      System.out.println(getBeforeHour());
    }
}
