package com.flink.feature;

import com.alibaba.alink.operator.stream.StreamOperator;
import lombok.Data;


import java.io.Serializable;

@Data
public class BaseSource   implements Serializable {
    public Long eventTime;
    public String logType;
    public String content;
    public Long rowTime;
    @Override
    public String toString(){

        return   "logType=" + logType +  " eventTime= "  +eventTime +" rowTime= " + rowTime ;
    }
}
