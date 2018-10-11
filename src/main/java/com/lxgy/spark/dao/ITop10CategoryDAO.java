package com.lxgy.spark.dao;

import com.lxgy.spark.domain.Top10Category;

/**
 * Top10模块DAO接口
 * @author Gryant
 *
 */
public interface ITop10CategoryDAO {

	/**
	 * 插入Top10
	 * @param top10Category
	 */
	void insert(Top10Category top10Category);
	
}
