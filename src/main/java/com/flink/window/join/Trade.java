package com.flink.window.join;

import com.flink.window.util.TimeStampUtils;


import java.sql.Timestamp;
import java.text.ParseException;


public class Trade {

    private String name;

    private long amount;

    private String client;

    private Timestamp tradeTime;

    public Trade() {
    }


    public Trade(String name, long amount, String client, String tradeTime) throws ParseException {
        this.name = name;
        this.amount = amount;
        this.client = client;
        this.tradeTime = TimeStampUtils.stringToTime(tradeTime);
    }

    public Timestamp getTradeTime() {
        return tradeTime;
    }

    public void setTradeTime(Timestamp tradeTime) {
        this.tradeTime = tradeTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "name='" + name + '\'' +
                ", amount=" + amount +
                ", client='" + client + '\'' +
                ", tradeTime=" + tradeTime +
                '}';
    }
}