package com.lxgy.spark.dao.impl;


import com.lxgy.spark.dao.IAdBlackListDAO;
import com.lxgy.spark.dao.IAreaTop3ProductDAO;
import com.lxgy.spark.domain.AdBlackList;
import com.lxgy.spark.domain.AreaTop3Product;
import com.lxgy.spark.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 广告黑名单DAO实现类
 * @author Gryant
 *
 */
public class AdBlackListDAOImpl implements IAdBlackListDAO {

	@Override
	public void insertBatch(List<AdBlackList> adBlackLists) {

		String sql = "insert into ad_blacklist values(?)";

		List<Object[]> params = new ArrayList<>();

		for (AdBlackList adBlackList : adBlackLists) {

			Object[] param = new Object[]{
					adBlackList.getUserId()
			};

			params.add(param);
		}

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, params);
	}

	@Override
	public List<AdBlackList> findAll() {

		String sql = "select * from ad_blacklist ";

		final List<AdBlackList> adBlackLists = new ArrayList<>();

		JDBCHelper.getInstance().executeQuery(sql, null, new JDBCHelper.QueryCallback() {
			@Override
			public void process(ResultSet rs) throws Exception {
				if (rs.next()) {
					adBlackLists.add(new AdBlackList(rs.getInt(1)));
				}
			}
		});

		return adBlackLists;
	}
}
