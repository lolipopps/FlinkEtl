package com.flink.bean;

import lombok.Data;

@Data
public class ClickSchemaEvent {

    public int userId;

    public String url;

    public String date;

    public long timeStamp;

    public String deviceType;

    public long count = 1;



}