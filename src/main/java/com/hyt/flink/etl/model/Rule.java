package com.hyt.flink.etl.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Rule implements Serializable {
    /**
     * 规则主键
     */
    private String id;

    /**
     * 规则名称
     */
    private String code;


    /**
     * 规则名称
     */
    private String name;

    /**
     * 规则类型
     */
    private String type;

    /**
     * 字段列表
     */
    private String fields;

    /**
     * 类型列表
     */
    private String columnTypes;

    /**
     * 描述列表
     */
    private String comments;

    /**
     * 分隔符
     */
    private String firstSplit;

    /**
     * 分隔符
     */
    private String secondSplit;

    /**
     * 开始位置
     */
    private String begin;
    /**
     * 规则编码
     */
    private String tableCode;


}
