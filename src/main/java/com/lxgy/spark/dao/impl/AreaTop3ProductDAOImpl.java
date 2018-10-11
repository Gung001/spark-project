package com.lxgy.spark.dao.impl;


import com.lxgy.spark.dao.IAreaTop3ProductDAO;
import com.lxgy.spark.dao.ISessionDetailDAO;
import com.lxgy.spark.domain.AreaTop3Product;
import com.lxgy.spark.domain.SessionDetail;
import com.lxgy.spark.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * 地区top3热门商品DAO实现类
 * @author Gryant
 *
 */
public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO {

	@Override
	public void insertBatch(List<AreaTop3Product> areaTop3Products) {

		String sql = "insert into area_top3_product values(?,?,?,?,?,?,?,?)";

		List<Object[]> params = new ArrayList<>();

		for (AreaTop3Product areaTop3Product : areaTop3Products) {

			Object[] param = new Object[]{
					areaTop3Product.getTaskId(),
					areaTop3Product.getArea(),
					areaTop3Product.getAreaLevel(),
					areaTop3Product.getProductId(),
					areaTop3Product.getCityInfos(),
					areaTop3Product.getClickCount(),
					areaTop3Product.getProductName(),
					areaTop3Product.getProductStatus()
					};

			params.add(param);
		}

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, params);
	}

}
