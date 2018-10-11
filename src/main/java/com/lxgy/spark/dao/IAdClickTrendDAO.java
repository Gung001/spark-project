package com.lxgy.spark.dao;

import com.lxgy.spark.domain.AdClickTrend;
import com.lxgy.spark.domain.AdStat;

import java.util.List;

/**
 * 各广告最近1小时点击趋势 DAO接口
 * @author Gryant
 *
 */
public interface IAdClickTrendDAO {

	/**
	 * 保存各广告最近1小时点击趋势
	 *
	 * @param adClickTrends
	 */
	void updateBatch(List<AdClickTrend> adClickTrends);
}
