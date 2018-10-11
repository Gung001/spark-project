package com.lxgy.spark.dao.impl;

import com.lxgy.spark.dao.IAdUserClickCountDAO;
import com.lxgy.spark.dao.ITaskDAO;
import com.lxgy.spark.domain.AdUserClickCount;
import com.lxgy.spark.domain.Task;
import com.lxgy.spark.jdbc.JDBCHelper;
import com.lxgy.spark.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 用户点击广告次数统计DAO实现类
 * @author Gryant
 *
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {

	@Override
	public void updateBatch(List<AdUserClickCount> adUserClickCounts) {

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();

		// 对用户广告进行分类：待插入 and 待更新
		List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<>();
		List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<>();

		String selectSQL = "select count(0) from ad_user_click_count " +
				"where date = ? and user_id = ? and ad_id = ?";

		Object[] selectParams = null;

		for (AdUserClickCount adUserClickCount : adUserClickCounts) {

			final AdUserClickCountQueryResult result = new AdUserClickCountQueryResult();

			selectParams = new Object[]{adUserClickCount.getDate(), adUserClickCount.getUserId(), adUserClickCount.getAdId()};

			jdbcHelper.executeQuery(selectSQL, selectParams, new JDBCHelper.QueryCallback() {
				@Override
				public void process(ResultSet rs) throws Exception {
					if (rs.next()) {
						int count = rs.getInt(1);
						result.setCount(count);
					}
				}
			});

			Integer count = result.getCount();

			if (count > 0) {
				updateAdUserClickCounts.add(adUserClickCount);
			} else {
				insertAdUserClickCounts.add(adUserClickCount);
			}
		}


		// 执行批量插入
		String insertSQL = "insert into ad_user_click_count values(?,?,?,?)";
		List<Object[]> insertParamsList = new ArrayList<>();

		for (AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
			Object[] insertParams = new Object[]{
					adUserClickCount.getDate(),
					adUserClickCount.getUserId(),
					adUserClickCount.getAdId(),
					adUserClickCount.getClickCount()
			};

			insertParamsList.add(insertParams);
		}

		jdbcHelper.executeBatch(insertSQL, insertParamsList);



		// 执行批量修改（点击次数需要在上一次的基础上累加）
		String updateSQL = "update ad_user_click_count set click_count = click_count + ? " +
				"where date = ? and user_id = ? and ad_id = ?";
		List<Object[]> updateParamsList = new ArrayList<>();

		for (AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
			Object[] updateParams = new Object[]{
					adUserClickCount.getClickCount(),
					adUserClickCount.getDate(),
					adUserClickCount.getUserId(),
					adUserClickCount.getAdId()
			};

			updateParamsList.add(updateParams);
		}

		jdbcHelper.executeBatch(updateSQL, updateParamsList);
	}

	@Override
	public int findClickCountByMultiKey(String date, Integer userId, Integer adId) {

		String sql = "select click_count from ad_user_click_count " +
				"where date = ? and user_id = ? and ad_id = ? ";

		Object[] params = new Object[]{date, userId, adId};

		final AdUserClickCountQueryResult result = new AdUserClickCountQueryResult();

		JDBCHelper.getInstance().executeQuery(sql, params, new JDBCHelper.QueryCallback() {
			@Override
			public void process(ResultSet rs) throws Exception {
				if (rs.next()) {
					int count = rs.getInt(1);
					result.setCount(count);
				}
			}
		});

		return result.getCount();
	}

}
