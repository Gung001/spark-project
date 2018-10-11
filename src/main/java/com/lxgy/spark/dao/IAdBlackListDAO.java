package com.lxgy.spark.dao;

import com.lxgy.spark.domain.AdBlackList;
import com.lxgy.spark.domain.AreaTop3Product;

import java.util.List;

/**
 * 广告黑名单 DAO接口
 * @author Gryant
 *
 */
public interface IAdBlackListDAO {

	/**
	 * 插入多条广告黑名单数据
	 * @param adBlackLists
	 */
	void insertBatch(List<AdBlackList> adBlackLists);

	/**
	 * 查询所有的黑名单用户
	 * @return
	 */
	List<AdBlackList> findAll();
	
}
