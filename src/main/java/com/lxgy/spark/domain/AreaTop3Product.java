package com.lxgy.spark.domain;

/**
 * @author Gryant
 */
public class AreaTop3Product {

    private Integer taskId;
    private String area;
    private String areaLevel;
    private Integer productId;
    private String cityInfos;
    private Integer clickCount;
    private String productName;
    private String productStatus;


    public AreaTop3Product(Integer taskId, String area, String areaLevel, Integer productId, String cityInfos, Integer clickCount, String productName, String productStatus) {
        this.taskId = taskId;
        this.area = area;
        this.areaLevel = areaLevel;
        this.productId = productId;
        this.cityInfos = cityInfos;
        this.clickCount = clickCount;
        this.productName = productName;
        this.productStatus = productStatus;
    }

    public AreaTop3Product() {

    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getAreaLevel() {
        return areaLevel;
    }

    public void setAreaLevel(String areaLevel) {
        this.areaLevel = areaLevel;
    }

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
    }

    public String getCityInfos() {
        return cityInfos;
    }

    public void setCityInfos(String cityInfos) {
        this.cityInfos = cityInfos;
    }

    public Integer getClickCount() {
        return clickCount;
    }

    public void setClickCount(Integer clickCount) {
        this.clickCount = clickCount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductStatus() {
        return productStatus;
    }

    public void setProductStatus(String productStatus) {
        this.productStatus = productStatus;
    }
}