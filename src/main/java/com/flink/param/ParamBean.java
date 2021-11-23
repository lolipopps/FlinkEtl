package com.flink.param;

import java.io.Serializable;

/**

 */
public class ParamBean implements Serializable {

    private String name;

    private int flag;

    /**

     */
    public ParamBean(String name, int flag) {
        this.name = name;
        this.flag = flag;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "ParamBean{" +
                "name='" + name + '\'' +
                ", flag=" + flag +
                '}';
    }
}
