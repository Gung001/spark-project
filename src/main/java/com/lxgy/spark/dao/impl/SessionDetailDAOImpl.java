package com.lxgy.spark.dao.impl;


import com.lxgy.spark.dao.ISessionDetailDAO;
import com.lxgy.spark.domain.SessionDetail;
import com.lxgy.spark.jdbc.JDBCHelper;

/**
 * session明细DAO实现类
 * @author Gryant
 *
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail 
	 */
	@Override
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{sessionDetail.getTaskId(),
				sessionDetail.getUserId(),
				sessionDetail.getSessionId(),
				sessionDetail.getPageId(),
				sessionDetail.getActionTime(),
				sessionDetail.getSearchKeyword(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds()};  
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	
}
