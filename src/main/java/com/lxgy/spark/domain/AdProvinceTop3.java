package com.lxgy.spark.domain;

/**
 * @author Gryant
 */
public class AdProvinceTop3 {

    private String date;
    private String province;
    private Integer adId;
    private Integer clickCount;

    public AdProvinceTop3() {
    }

    public AdProvinceTop3(String date, String province, Integer adId, Integer clickCount) {

        this.date = date;
        this.province = province;
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