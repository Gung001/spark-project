package com.lxgy.spark.dao;

import com.lxgy.spark.domain.AdProvinceTop3;
import com.lxgy.spark.domain.AdStat;

import java.util.List;

/**
 * 实时各省份top3热门广告 DAO接口
 * @author Gryant
 *
 */
public interface IAdProvinceTop3DAO {

	/**
	 * 保存 实时各省份top3热门广告
	 *
	 * @param adProvinceTop3s
	 */
	void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}
