package com.lxgy.spark.dao;

import com.lxgy.spark.domain.AdUserClickCount;
import com.lxgy.spark.domain.Top10Category;

import java.util.List;

/**
 * 用户点击广告次数统计模块DAO接口
 * @author Gryant
 *
 */
public interface IAdUserClickCountDAO {

	/**
	 * 插入Top10
	 *
	 * @param adUserClickCounts
	 */
	void updateBatch(List<AdUserClickCount> adUserClickCounts);
	
}
