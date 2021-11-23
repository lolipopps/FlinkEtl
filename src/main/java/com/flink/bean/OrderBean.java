package com.flink.bean;


public class OrderBean {

    public Long user;
    public String product;
    public int amount;

    public OrderBean() {
    }


    public OrderBean(Long user, String product, int amount) {
        this.user = user;
        this.product = product;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }
}