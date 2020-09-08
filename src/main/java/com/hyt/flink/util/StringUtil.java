package com.hyt.flink.util;

/**
 * @Author: hyt
 * @License: (C) Copyright 2020-2020, xxx Corporation Limited.
 * @Contact: xxx@xxx.com
 * @Version: 1.0
 * @Description: 往kafka中写数据,可以使用这个main函数进行测试
 */
public class StringUtil {
    /**
     * 判空
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }

    /**
     * 判非空
     *
     * @param str
     * @return
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * 包含
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean isContains(String str1, String str2) {
        return str1.contains(str2);
    }

    /**
     * 不包含
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean isNotContains(String str1, String str2) {
        return !isContains(str1, str2);
    }

    public static void main(String[] args){
        String split = "\\t";
        String source ="123243\tewqwretr\tdfsdbgf\t";
        String res = source.replaceAll(split, "\\$\\$");
        //      System.out.println(res);
        source =  source.replaceAll(  split,"\\$\\$");
    }
}
