package com.lxgy.spark.dao.impl;

import com.lxgy.spark.dao.IAdStatDAO;
import com.lxgy.spark.domain.AdStat;
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
public class AdStatDAOImpl implements IAdStatDAO {

    @Override
    public void updateBatch(List<AdStat> adStats) {

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 实时广告计算统计进行分类：待插入 and 待更新
        List<AdStat> insertAdStats = new ArrayList<>();
        List<AdStat> updateAdStats = new ArrayList<>();

        String selectSQL = "select count(0) from ad_stat " +
                "where date = ? and province = ? and city = ? and ad_id = ?";

        Object[] selectParams = null;

        for (AdStat adStat : adStats) {

            final AdUserClickCountQueryResult result = new AdUserClickCountQueryResult();

            selectParams = new Object[]{adStat.getDate(), adStat.getProvince(), adStat.getCity(), adStat.getAdId()};

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
                updateAdStats.add(adStat);
            } else {
                insertAdStats.add(adStat);
            }
        }


        // 执行批量插入
        String insertSQL = "insert into ad_stat values(?,?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<>();

        for (AdStat adStat : insertAdStats) {
            Object[] insertParams = new Object[]{
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId(),
                    adStat.getClickCount()
            };

            insertParamsList.add(insertParams);
        }

        jdbcHelper.executeBatch(insertSQL, insertParamsList);


        // 执行批量修改（点击次数需要在上一次的基础上累加）
        String updateSQL = "update ad_stat set click_count = click_count + ? " +
                "where date = ? and province = ? and city = ? and ad_id = ?";
        List<Object[]> updateParamsList = new ArrayList<>();

        for (AdStat AdStat : insertAdStats) {
            Object[] updateParams = new Object[]{
                    AdStat.getClickCount(),
                    AdStat.getDate(),
                    AdStat.getProvince(),
                    AdStat.getCity(),
                    AdStat.getAdId()
            };

            updateParamsList.add(updateParams);
        }

        jdbcHelper.executeBatch(updateSQL, updateParamsList);
    }
}
