package com.flink.window.time.bean;

import com.flink.window.util.TimeUtils;


import java.util.ArrayList;
import java.util.List;


public class EventBean {

    private List<String> list;

    private long time;


    public EventBean(String text, long time) {
        list = new ArrayList<String>();
        list.add(text);
        this.time = time;
    }


    public EventBean() {
    }

    public long getTime() {
        return time;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    public void setTime(long time) {
        this.time = time;
    }


    @Override
    public String toString() {
        return "{" +
                "text='" + list.toString() + '\'' +
                ", time=" + TimeUtils.getHHmmss(time) +
                '}';
    }
}
