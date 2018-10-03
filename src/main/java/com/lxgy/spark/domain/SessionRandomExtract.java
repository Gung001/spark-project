package com.lxgy.spark.domain;

import java.io.Serializable;

/**
 * @author Gryant
 */
public class SessionRandomExtract implements Serializable {
    private static final long serialVersionUID = 42L;

    /**
     * task_id
     */
    private Integer taskId;

    /**
     * session_id
     */
    private String sessionId;

    /**
     * start_time
     */
    private String startTime;

    /**
     * click_category_ids
     */
    private String clickCategoryIds;

    /**
     * search_keywords
     */
    private String searchKeywords;


    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getClickCategoryIds() {
        return clickCategoryIds;
    }

    public void setClickCategoryIds(String clickCategoryIds) {
        this.clickCategoryIds = clickCategoryIds;
    }

    public String getSearchKeywords() {
        return searchKeywords;
    }

    public void setSearchKeywords(String searchKeywords) {
        this.searchKeywords = searchKeywords;
    }




}
