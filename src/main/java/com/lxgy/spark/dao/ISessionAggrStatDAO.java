package com.lxgy.spark.dao;


import com.lxgy.spark.domain.SessionAggrStat;

/**
 * 聚合统计结果DAO接口
 * @author Gryant
 *
 */
public interface ISessionAggrStatDAO {
	
	/**
	 * 添加session聚合统计结果
	 * @param sessionAggrStat
	 * @return 任务
	 */
	void add(SessionAggrStat sessionAggrStat);
	
}
