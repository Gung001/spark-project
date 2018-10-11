package com.lxgy.spark.dao;

import com.lxgy.spark.domain.SessionDetail;

import java.util.List;

/**
 * Session明细DAO接口
 * @author Gryant
 *
 */
public interface ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail 
	 */
	void insert(SessionDetail sessionDetail);

	/**
	 * 插入多条session明细数据
	 * @param sessionDetails
	 */
	void insertBatch(List<SessionDetail> sessionDetails);
	
}
