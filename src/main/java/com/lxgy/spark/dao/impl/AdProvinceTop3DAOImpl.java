package com.lxgy.spark.dao.impl;

import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.dao.IAdProvinceTop3DAO;
import com.lxgy.spark.domain.AdProvinceTop3;
import com.lxgy.spark.jdbc.JDBCHelper;
import org.apache.calcite.sql.validate.CollectNamespace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 实时广告计算统计 DAO实现类
 *
 * @author Gryant
 */
public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {

    @Override
    public void updateBatch(List<AdProvinceTop3> adProvinceTop3s) {

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 删除原有的数据
        List<AdProvinceTop3> distinctAdProvinceTop3 = new ArrayList<>();
        List<String> distinctKeyList = new ArrayList<>();
        for (AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
            String province = adProvinceTop3.getProvince();
            String date = adProvinceTop3.getDate();
            String key = date + Constants.SPLIT_SYMBAL_UNDERLINE_BAR + province;

            // 去重后需要新增的数据集合
            if (!distinctKeyList.contains(key)) {
                distinctAdProvinceTop3.add(adProvinceTop3);
                distinctKeyList.add(key);
            }
        }

        // 执行批量插入
        String insertSQL = "insert into ad_stat values(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<>();

        for (AdProvinceTop3 adStat : distinctAdProvinceTop3) {
            Object[] insertParams = new Object[]{
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getAdId(),
                    adStat.getClickCount()
            };

            insertParamsList.add(insertParams);
        }

        jdbcHelper.executeBatch(insertSQL, insertParamsList);
    }
}
