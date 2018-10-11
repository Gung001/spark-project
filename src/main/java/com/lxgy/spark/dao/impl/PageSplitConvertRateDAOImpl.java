package com.lxgy.spark.dao.impl;

import com.lxgy.spark.dao.IPageSplitConvertRateDAO;
import com.lxgy.spark.domain.PageSplitConvertRate;
import com.lxgy.spark.jdbc.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 *
 * @author Gryant
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {

	@Override
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";  
		Object[] params = new Object[]{pageSplitConvertRate.getTaskId(),
				pageSplitConvertRate.getConvertRate()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
