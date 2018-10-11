package com.lxgy.spark.domain;

/**
 * @author Gryant
 */
public class AdStat {

    private String date;
    private String province;
    private String city;
    private Integer adId;
    private Integer clickCount;

    public AdStat() {
    }

    public AdStat(String date, String province, String city, Integer adId, Integer clickCount) {

        this.date = date;
        this.province = province;
        this.city = city;
        this.adId = adId;
        this.clickCount = clickCount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
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