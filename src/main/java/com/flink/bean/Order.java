package com.flink.bean;

import com.flink.util.TimeStampUtils;

import java.sql.Timestamp;
import java.text.ParseException;


public class Order {

    public String currency;

    public Timestamp time;

    public int amount;

    public int id;

    public Order() {
    }


    public Order(int id, String currency, String time, int amount) throws ParseException {
        this.currency = currency;
        this.time = TimeStampUtils.stringToTime(time);
        this.amount = amount;
        this.id = id;
    }


    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
