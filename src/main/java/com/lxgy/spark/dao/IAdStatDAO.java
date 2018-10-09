package com.lxgy.spark.dao;

import com.lxgy.spark.domain.AdStat;

import java.util.List;

/**
 * 实时广告点击量 DAO接口
 * @author Gryant
 *
 */
public interface IAdStatDAO {

	/**
	 * 插入Top10
	 *
	 * @param adStats
	 */
	void updateBatch(List<AdStat> adStats);
}
