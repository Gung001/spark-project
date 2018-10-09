package com.lxgy.spark.dao.impl;

import com.lxgy.spark.dao.IAdClickTrendDAO;
import com.lxgy.spark.domain.AdClickTrend;
import com.lxgy.spark.jdbc.JDBCHelper;
import com.lxgy.spark.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 实时广告计算统计 DAO实现类
 *
 * @author Gryant
 */
public class AdClickTrendDAOImpl implements IAdClickTrendDAO {

    @Override
    public void updateBatch(List<AdClickTrend> adClickTrends) {

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 进行分类：待插入 and 待更新
        List<AdClickTrend> insertAdClickTrend = new ArrayList<>();
        List<AdClickTrend> updateAdClickTrend = new ArrayList<>();

        String selectSQL = "select count(0) from ad_click_trend " +
                "where date = ? and hour = ? and minute = ? and ad_id = ?";

        Object[] selectParams = null;

        for (AdClickTrend adClickTrend : adClickTrends) {

            final AdUserClickCountQueryResult result = new AdUserClickCountQueryResult();

            selectParams = new Object[]{
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdId()};

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
                updateAdClickTrend.add(adClickTrend);
            } else {
                insertAdClickTrend.add(adClickTrend);
            }
        }


        // 执行批量插入
        String insertSQL = "insert into ad_click_trend values(?,?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<>();

        for (AdClickTrend adStat : insertAdClickTrend) {
            Object[] insertParams = new Object[]{
                    adStat.getDate(),
                    adStat.getHour(),
                    adStat.getMinute(),
                    adStat.getAdId(),
                    adStat.getClickCount()
            };

            insertParamsList.add(insertParams);
        }

        jdbcHelper.executeBatch(insertSQL, insertParamsList);


        // 执行批量修改（点击次数需要在上一次的基础上累加）
        String updateSQL = "update ad_click_trend set click_count = click_count + ? " +
                "where date = ? and hour = ? and minute = ? and ad_id = ?";
        List<Object[]> updateParamsList = new ArrayList<>();

        for (AdClickTrend adClickTrend : insertAdClickTrend) {
            Object[] updateParams = new Object[]{
                    adClickTrend.getClickCount(),
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdId()
            };

            updateParamsList.add(updateParams);
        }

        jdbcHelper.executeBatch(updateSQL, updateParamsList);
    }
}
