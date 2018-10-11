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
	 * 保存广告点击量
	 *
	 * @param adUserClickCounts
	 */
	void updateBatch(List<AdUserClickCount> adUserClickCounts);

	/**
	 * 查询key下对应的点击次数
	 * @param date
	 * @param userId
	 * @param adId
	 * @return
	 */
	int findClickCountByMultiKey(String date, Integer userId, Integer adId);
}
