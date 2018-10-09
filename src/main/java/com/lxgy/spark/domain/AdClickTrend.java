package com.lxgy.spark.domain;

/**
 * @author Gryant
 */
public class AdClickTrend {

    private String date;
    private String hour;
    private String minute;
    private Integer adId;
    private Integer clickCount;


    public AdClickTrend(String date, String hour, String minute, Integer adId, Integer clickCount) {
        this.date = date;
        this.hour = hour;
        this.minute = minute;
        this.adId = adId;
        this.clickCount = clickCount;
    }
    public AdClickTrend() {
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
    }

    public Integer getAdId() {
        return adId;
    }

    public void setAdId(Integer adId) {
        this.adId = adId;
    }

    public Integer getClickCount() {
        return clickCount;
    }

    public void setClickCount(Integer clickCount) {
        this.clickCount = clickCount;
    }
}
