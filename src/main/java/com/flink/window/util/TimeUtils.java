package com.flink.window.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**

 * @date: 2020/10/15 18:33
 */
public class TimeUtils {

    /**

     * @date: 2020/10/15 18:33
     */
    public static String getHHmmss(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss SSS");
        String str = sdf.format(date);
        return "时间:" + str;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    public static String getHHmmss(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String str = sdf.format(new Date(time));
        return "时间:" + str;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    public static String getSs(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss SSS");
        String str = sdf.format(date);
        return "时间:" + str;
    }

    /**

     * @date: 2020/10/15 18:33
     */
    public static String getSs(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss SSS");
        String str = sdf.format(new Date(time));
        return str;
    }

}
