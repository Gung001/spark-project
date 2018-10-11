package com.lxgy.spark.dao;

import com.lxgy.spark.domain.AreaTop3Product;
import com.lxgy.spark.domain.SessionDetail;

import java.util.List;

/**
 * top3 地区热门商品 DAO接口
 * @author Gryant
 *
 */
public interface IAreaTop3ProductDAO {

	/**
	 * 插入多条op3 地区热门商品数据
	 * @param areaTop3Products
	 */
	void insertBatch(List<AreaTop3Product> areaTop3Products);
	
}
