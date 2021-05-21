package com.flink.bean;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;


@Data
public class EventSchemaBean {

    public int userId;

    public String date;

    public long timeStamp;

    public List<String> list = new ArrayList<String>();


}