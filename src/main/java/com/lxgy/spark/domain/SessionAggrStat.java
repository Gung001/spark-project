package com.lxgy.spark.domain;

/**
 * @author Gryant
 */
public class SessionAggrStat {

    private static final long serialVersionUID = 42L;

    /**
     * task_id
     */
    private Integer taskId;

    /**
     * session_count
     */
    private Integer sessionCount;

    /**
     * 1s_3s
     */
    private Double visit_length_1s_3s;

    /**
     * 4s_6s
     */
    private Double visit_length_4s_6s;

    /**
     * 7s_9s
     */
    private Double visit_length_7s_9s;

    /**
     * 10s_30s
     */
    private Double visit_length_10s_30s;

    /**
     * 30s_60s
     */
    private Double visit_length_30s_60s;

    /**
     * 1m_3m
     */
    private Double visit_length_1m_3m;

    /**
     * 3m_10m
     */
    private Double visit_length_3m_10m;

    /**
     * 10m_30m
     */
    private Double visit_length_10m_30m;

    /**
     * 30m
     */
    private Double visit_length_30m;

    /**
     * 1_3
     */
    private Double step_length_1_3;

    /**
     * 4_6
     */
    private Double step_length_4_6;

    /**
     * 7_9
     */
    private Double step_length_7_9;

    /**
     * 10_30
     */
    private Double step_length_10_30;

    /**
     * 30_60
     */
    private Double step_length_30_60;

    /**
     * 60
     */
    private Double step_length_60;

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public Integer getSessionCount() {
        return sessionCount;
    }

    public void setSessionCount(Integer sessionCount) {
        this.sessionCount = sessionCount;
    }

    public Double getVisit_length_1s_3s() {
        return visit_length_1s_3s;
    }

    public void setVisit_length_1s_3s(Double visit_length_1s_3s) {
        this.visit_length_1s_3s = visit_length_1s_3s;
    }

    public Double getVisit_length_4s_6s() {
        return visit_length_4s_6s;
    }

    public void setVisit_length_4s_6s(Double visit_length_4s_6s) {
        this.visit_length_4s_6s = visit_length_4s_6s;
    }

    public Double getVisit_length_7s_9s() {
        return visit_length_7s_9s;
    }

    public void setVisit_length_7s_9s(Double visit_length_7s_9s) {
        this.visit_length_7s_9s = visit_length_7s_9s;
    }

    public Double getVisit_length_10s_30s() {
        return visit_length_10s_30s;
    }

    public void setVisit_length_10s_30s(Double visit_length_10s_30s) {
        this.visit_length_10s_30s = visit_length_10s_30s;
    }

    public Double getVisit_length_30s_60s() {
        return visit_length_30s_60s;
    }

    public void setVisit_length_30s_60s(Double visit_length_30s_60s) {
        this.visit_length_30s_60s = visit_length_30s_60s;
    }

    public Double getVisit_length_1m_3m() {
        return visit_length_1m_3m;
    }

    public void setVisit_length_1m_3m(Double visit_length_1m_3m) {
        this.visit_length_1m_3m = visit_length_1m_3m;
    }

    public Double getVisit_length_3m_10m() {
        return visit_length_3m_10m;
    }

    public void setVisit_length_3m_10m(Double visit_length_3m_10m) {
        this.visit_length_3m_10m = visit_length_3m_10m;
    }

    public Double getVisit_length_10m_30m() {
        return visit_length_10m_30m;
    }

    public void setVisit_length_10m_30m(Double visit_length_10m_30m) {
        this.visit_length_10m_30m = visit_length_10m_30m;
    }

    public Double getVisit_length_30m() {
        return visit_length_30m;
    }

    public void setVisit_length_30m(Double visit_length_30m) {
        this.visit_length_30m = visit_length_30m;
    }

    public Double getStep_length_1_3() {
        return step_length_1_3;
    }

    public void setStep_length_1_3(Double step_length_1_3) {
        this.step_length_1_3 = step_length_1_3;
    }

    public Double getStep_length_4_6() {
        return step_length_4_6;
    }

    public void setStep_length_4_6(Double step_length_4_6) {
        this.step_length_4_6 = step_length_4_6;
    }

    public Double getStep_length_7_9() {
        return step_length_7_9;
    }

    public void setStep_length_7_9(Double step_length_7_9) {
        this.step_length_7_9 = step_length_7_9;
    }

    public Double getStep_length_10_30() {
        return step_length_10_30;
    }

    public void setStep_length_10_30(Double step_length_10_30) {
        this.step_length_10_30 = step_length_10_30;
    }

    public Double getStep_length_30_60() {
        return step_length_30_60;
    }

    public void setStep_length_30_60(Double step_length_30_60) {
        this.step_length_30_60 = step_length_30_60;
    }

    public Double getStep_length_60() {
        return step_length_60;
    }

    public void setStep_length_60(Double step_length_60) {
        this.step_length_60 = step_length_60;
    }
}
