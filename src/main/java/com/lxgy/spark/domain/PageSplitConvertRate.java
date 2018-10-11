package com.lxgy.spark.domain;

/**
 * 页面切片转化率
 * @author Gryant
 *
 */
public class PageSplitConvertRate {

	private long taskId;
	private String convertRate;

	public PageSplitConvertRate() {}

	public PageSplitConvertRate(long taskId, String convertRate) {
		this.taskId = taskId;
		this.convertRate = convertRate;
	}

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public String getConvertRate() {
		return convertRate;
	}
	public void setConvertRate(String convertRate) {
		this.convertRate = convertRate;
	}
	
}
