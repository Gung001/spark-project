package com.lxgy.spark.dao;

import com.lxgy.spark.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 * @author Gryant
 *
 */
public interface ISessionRandomExtractDAO {

	/**
	 * 插入session随机抽取
	 * @param sessionRandomExtract
	 */
	void insert(SessionRandomExtract sessionRandomExtract);
	
}
