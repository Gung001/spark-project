package com.lxgy.spark.utils;

import java.math.BigDecimal;

/**
 * 数字格工具类
 *
 * @author Gryant
 */
public class NumberUtils {

    /**
     * 格式化小数
     *
     * @param num   字符串
     * @param scale 四舍五入的位数
     * @return 格式化小数
     */
    public static double formatDouble(double num, int scale) {
        BigDecimal bd = new BigDecimal(num);
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }


    /**
     * 格式化小数
     *
     * @param obj1 被除数
     * @param obj2 除数
     * @return 格式化小数
     */
    public static BigDecimal divOp(Object obj1, Object obj2, Integer scale) {

        BigDecimal bigDecimal = new BigDecimal(0);

        if (obj1 == null || obj2 == null) {
            return bigDecimal;
        }

        if (scale == null) {
            scale = 2;
        }

        BigDecimal divedDecimal = new BigDecimal(String.valueOf(obj2));
        if (divedDecimal.compareTo(bigDecimal) == 0) {
            return bigDecimal;
        }

        return new BigDecimal(String.valueOf(obj1)).divide(divedDecimal, scale, BigDecimal.ROUND_HALF_UP);
    }

}
