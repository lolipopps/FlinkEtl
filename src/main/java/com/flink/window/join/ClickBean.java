package com.flink.window.join;

import com.flink.window.util.TimeStampUtils;


import java.sql.Timestamp;
import java.text.ParseException;

/**

 * @date: 2020/10/15 18:33
 */
public class ClickBean {

    public String user;

    public Timestamp visitTime;

    public String url;

    public int id;

    public ClickBean() {
    }

    /**

     * @date: 2020/10/15 18:33
     */
    public ClickBean(int id, String user, String url, String visitTime) throws ParseException {
        this.user = user;
        this.url = url;
        this.visitTime = TimeStampUtils.stringToTime(visitTime);
        this.id = id;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Timestamp getVisitTime() {
        return visitTime;
    }

    public void setVisitTime(Timestamp visitTime) {
        this.visitTime = visitTime;
    }

    @Override
    public String toString() {
        return "ClickBean{" +
                "user='" + user + '\'' +
                ", visitTime=" + visitTime +
                ", url='" + url + '\'' +
                '}';
    }
}
