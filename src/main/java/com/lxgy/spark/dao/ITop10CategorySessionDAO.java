package com.lxgy.spark.dao;

import com.lxgy.spark.domain.Top10CategorySession;

/**
 * Top10session模块DAO接口
 * @author Gryant
 *
 */
public interface ITop10CategorySessionDAO {

	/**
	 * 插入Top10session
	 * @param top10CategorySession
	 */
	void insert(Top10CategorySession top10CategorySession);
	
}
