package com.lxgy.spark.domain;

/**
 * @author Gryant
 */
public class AdUserClickCount {

    private String date;
    private Integer userId;
    private Integer adId;
    private Integer clickCount;

    public AdUserClickCount() {
    }

    public AdUserClickCount(String date, Integer userId, Integer adId, Integer clickCount) {
        this.date = date;
        this.userId = userId;
        this.adId = adId;
        this.clickCount = clickCount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
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