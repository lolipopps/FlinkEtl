package com.flink.bean;

import lombok.Data;

@Data
public class OrderSchemaEvent {

    public int userId;

    public String url;

    public String deviceType;

    public long count = 1;


}