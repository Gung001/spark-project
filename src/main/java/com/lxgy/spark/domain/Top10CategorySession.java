package com.lxgy.spark.domain;

/**
 * @author Gryant
 */
public class Top10CategorySession {

    private Integer taskId;
    private Integer categoryId;
    private String sessionId;
    private Integer clickCount;

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

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Integer getClickCount() {
        return clickCount;
    }

    public void setClickCount(Integer clickCount) {
        this.clickCount = clickCount;
    }

    public Top10CategorySession() {
    }

    public Top10CategorySession(Integer taskId, Integer categoryId, String sessionId, Integer clickCount) {
        this.taskId = taskId;
        this.categoryId = categoryId;
        this.sessionId = sessionId;
        this.clickCount = clickCount;
    }
}
