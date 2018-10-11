package com.lxgy.spark.dao.impl;


import com.lxgy.spark.dao.ITop10CategoryDAO;
import com.lxgy.spark.domain.Top10Category;
import com.lxgy.spark.jdbc.JDBCHelper;

/**
 * Top10的DAO实现
 *
 * @author Gryant
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    @Override
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{top10Category.getTaskId(),
                top10Category.getCategoryId(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getPayCount()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
