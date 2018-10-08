package com.lxgy.spark.dao.impl;


import com.lxgy.spark.dao.ISessionRandomExtractDAO;
import com.lxgy.spark.domain.SessionRandomExtract;
import com.lxgy.spark.jdbc.JDBCHelper;

/**
 * 随机抽取session的DAO实现
 *
 * @author Gryant
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

    /**
     * 插入session随机抽取
     *
     * @param sessionRandomExtract
     */
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";

        Object[] params = new Object[]{sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getClickCategoryIds(),
                sessionRandomExtract.getSearchKeywords()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
