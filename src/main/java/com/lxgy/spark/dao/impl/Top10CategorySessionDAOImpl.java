package com.lxgy.spark.dao.impl;


import com.lxgy.spark.dao.ITop10CategorySessionDAO;
import com.lxgy.spark.domain.Top10CategorySession;
import com.lxgy.spark.jdbc.JDBCHelper;

/**
 * Top10session的DAO实现
 *
 * @author Gryant
 */
public class Top10CategorySessionDAOImpl implements ITop10CategorySessionDAO {

    @Override
    public void insert(Top10CategorySession top10CategorySession) {
        String sql = "insert into top10_category_session values(?,?,?,?)";

        Object[] params = new Object[]{top10CategorySession.getTaskId(),
                top10CategorySession.getCategoryId(),
                top10CategorySession.getSessionId(),
                top10CategorySession.getClickCount()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
