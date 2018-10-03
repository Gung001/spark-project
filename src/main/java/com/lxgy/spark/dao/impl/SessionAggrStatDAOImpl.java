package com.lxgy.spark.dao.impl;

import com.lxgy.spark.dao.ISessionAggrStatDAO;
import com.lxgy.spark.dao.ITaskDAO;
import com.lxgy.spark.domain.SessionAggrStat;
import com.lxgy.spark.domain.Task;
import com.lxgy.spark.jdbc.JDBCHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

/**
 * session聚合统计结果DAO实现类
 *
 * @author Gryant
 */
public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {

    private final static Logger logger = LoggerFactory.getLogger(SessionAggrStatDAOImpl.class);

    @Override
    public void add(SessionAggrStat sessionAggrStat) {

        String sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[]{
                sessionAggrStat.getTaskId(),
                sessionAggrStat.getSessionCount(),
                sessionAggrStat.getVisit_length_1s_3s(),
                sessionAggrStat.getVisit_length_4s_6s(),
                sessionAggrStat.getVisit_length_7s_9s(),
                sessionAggrStat.getVisit_length_10s_30s(),
                sessionAggrStat.getVisit_length_30s_60s(),
                sessionAggrStat.getVisit_length_1m_3m(),
                sessionAggrStat.getVisit_length_3m_10m(),
                sessionAggrStat.getVisit_length_10m_30m(),
                sessionAggrStat.getVisit_length_30m(),
                sessionAggrStat.getStep_length_1_3(),
                sessionAggrStat.getStep_length_4_6(),
                sessionAggrStat.getStep_length_7_9(),
                sessionAggrStat.getStep_length_10_30(),
                sessionAggrStat.getStep_length_30_60(),
                sessionAggrStat.getStep_length_60()
        };

        int executeUpdate = JDBCHelper.getInstance().executeUpdate(sql, params);

        logger.debug("SessionAggrStatDAOImpl.add result=" + executeUpdate);

    }
}
