package com.lxgy.spark.domain;

/**
 * @author Gryant
 */
public class AdBlackList {

    private Integer userId;

    public AdBlackList() {
    }

    public AdBlackList(Integer userId) {
        this.userId = userId;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }
}