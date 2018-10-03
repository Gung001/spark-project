package com.lxgy.spark.session;


import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.utils.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 *
 * session 聚合 accumulator
 * @author Gryant
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    /**
     * zero 方法主要用于数据初始化工作
     * @param initialValue
     * @return
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * addInPlace和addAccumulator
     * 可以理解为是一样的
     *
     * 这两个方法，其实主要就是实现，v1可能就是我们初始化的那个连接串
     * v2，就是我们在遍历session的时候，判断出某个session对应的区间，然后会用Constants.TIME_PERIOD_1s_3s
     * 所以，我们，要做的事情就是
     * 在v1中，找到v2对应的value，累加1，然后再更新回连接串里面去
     *
     */
    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    /**
     * session统计计算逻辑
     * @param v1 连接串
     * @param v2 范围区间
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2) {
        // 校验：v1为空的话，直接返回v2
        if(StringUtils.isEmpty(v1)) {
            return v2;
        }

        // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, Constants.SPLIT_SYMBAL_VERTICAL_BAR, v2);
        if(oldValue != null) {
            // 将范围区间原有的值，累加1
            int newValue = Integer.valueOf(oldValue) + 1;
            // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            return StringUtils.setFieldInConcatString(v1, Constants.SPLIT_SYMBAL_VERTICAL_BAR, v2, String.valueOf(newValue));
        }

        return v1;
    }

}
