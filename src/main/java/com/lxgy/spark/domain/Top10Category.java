package com.lxgy.spark.domain;

import java.io.Serializable;

/**
 * @author Gryant
 */
public class Top10Category implements Serializable {

    private Integer taskId;
    private Integer categoryId;
    private Integer clickCount;
    private Integer orderCount;
    private Integer payCount;

    public Top10Category() {

    }


    public Top10Category(Integer taskId, Integer categoryId, Integer clickCount, Integer orderCount, Integer payCount) {
        this.taskId = taskId;
        this.categoryId = categoryId;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public Integer getClickCount() {
        return clickCount;
    }

    public void setClickCount(Integer clickCount) {
        this.clickCount = clickCount;
    }

    public Integer getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Integer orderCount) {
        this.orderCount = orderCount;
    }

    public Integer getPayCount() {
        return payCount;
    }

    public void setPayCount(Integer payCount) {
        this.payCount = payCount;
    }
}
