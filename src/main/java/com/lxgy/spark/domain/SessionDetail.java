package com.lxgy.spark.domain;

import java.io.Serializable;

/**
 *  SessionDetail
 * @author Gryant
 */
public class SessionDetail implements Serializable {
    private static final long serialVersionUID = 42L;

    /**
     * task_id
     */
    private Integer taskId;

    /**
     * user_id
     */
    private Long userId;

    /**
     * session_id
     */
    private String sessionId;

    /**
     * page_id
     */
    private Long pageId;

    /**
     * action_time
     */
    private String actionTime;

    /**
     * search_keyword
     */
    private String searchKeyword;

    /**
     * click_category_id
     */
    private Long clickCategoryId;

    /**
     * click_product_id
     */
    private Long clickProductId;

    /**
     * order_category_ids
     */
    private String orderCategoryIds;

    /**
     * order_product_ids
     */
    private String orderProductIds;

    /**
     * pay_category_ids
     */
    private String payCategoryIds;

    /**
     * pay_product_ids
     */
    private String payProductIds;


    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Long getPageId() {
        return pageId;
    }

    public void setPageId(Long pageId) {
        this.pageId = pageId;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getSearchKeyword() {
        return searchKeyword;
    }

    public void setSearchKeyword(String searchKeyword) {
        this.searchKeyword = searchKeyword;
    }

    public Long getClickCategoryId() {
        return clickCategoryId;
    }

    public void setClickCategoryId(Long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public Long getClickProductId() {
        return clickProductId;
    }

    public void setClickProductId(Long clickProductId) {
        this.clickProductId = clickProductId;
    }

    public String getOrderCategoryIds() {
        return orderCategoryIds;
    }

    public void setOrderCategoryIds(String orderCategoryIds) {
        this.orderCategoryIds = orderCategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }

    public String getPayProductIds() {
        return payProductIds;
    }

    public void setPayProductIds(String payProductIds) {
        this.payProductIds = payProductIds;
    }

}