package com.flink.feature;

import lombok.Data;

import java.io.Serializable;

@Data
public class BaseSourceTable implements Serializable {

    String logType;
    Long eventTime;
    String content;
}
