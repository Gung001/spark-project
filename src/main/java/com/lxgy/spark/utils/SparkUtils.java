package com.lxgy.spark.utils;

import com.alibaba.fastjson.JSONObject;
import com.lxgy.spark.conf.ConfigurationManager;
import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.mock.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author Gryant
 */
public class SparkUtils {

    /*执行模式*/
    private static final Boolean local;

    static {
        // 读取配置文件，查看是否为本地模式
        local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
    }

    /**
     * 设置SparkConf的master
     * @param sparkConf
     */
    public static void setMaster(SparkConf sparkConf){
        if (local) {
            sparkConf.setMaster("local");
        }
    }

    /**
     * 如果是本地模式生成模拟数据
     * @param sc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return SQLContext
     */
    public static SQLContext getSQLContext(SparkContext sc) {
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext SQLContext
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    public static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext, JSONObject taskParam) {

        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        /**
         * 特别说明：
         * 这里 有可能发生Spark SQL默认给第一个stage设置了20个task，
         * 但是根据数据量以及算法的复杂度需要1000个task并行执行
         */
//        return actionDF.javaRDD().repartition(1000);

        return actionDF.javaRDD();
    }

}
