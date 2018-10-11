package com.lxgy.spark.product;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxgy.spark.conf.ConfigurationManager;
import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.dao.ITaskDAO;
import com.lxgy.spark.dao.impl.DAOFactory;
import com.lxgy.spark.domain.AreaTop3Product;
import com.lxgy.spark.domain.Task;
import com.lxgy.spark.utils.ParamUtils;
import com.lxgy.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Gryant
 */
public class AreaTop3ProductSpark {

    public static void main(String[] args) {

        // 构建Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
//        sqlContext.setConf("spark.sql.shuffle.partitions","1000");

        // 注册自定义函数
        sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);
        sqlContext.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
        sqlContext.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());

        // 生成模拟测试数据
        SparkUtils.mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 首先得查询出来指定的任务，并获取任务的查询参数
        Task task = taskDAO.findById(ParamUtils.getTaskIdFromArgs(args));
        if (task == null) {
            throw new RuntimeException("没有获取到指定的任务：" + JSON.toJSONString(args));
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        // 获取指定日期范围内的点击行为数据
        JavaPairRDD<Long,Row> cityId2ClickActionRDD = getCityId2ClickActionRDDByDate(sqlContext, startDate, endDate);

        // 从MySQL从获取城市信息RDD
        JavaPairRDD<Long,Row> cityId2CityInfoRDD = getCityId2CityInfoRDD(sqlContext);

        // 生成点击商品临时表
        generateTmpClickProductBasicTable(sqlContext, cityId2ClickActionRDD, cityId2CityInfoRDD);

        // 生成区域商品点击次数临时表
        generateTmpAreaProductClickCountTable(sqlContext);

        // 生成区域商品点击次数临时表
        generateTmpAreaFullProductClickCountTable(sqlContext);

        // 获取各区域top3
        JavaRDD<Row> areaTop3Product = getAreaTop3ProductRDD(sqlContext);

        // 写库
        persistAreaTop3Product(task.getTaskId(), areaTop3Product);

        sc.close();

    }

    /**
     * 持久化各区域top3
     * @param taskId
     * @param areaTop3Product
     */
    private static void persistAreaTop3Product(Integer taskId, JavaRDD<Row> areaTop3Product) {
        List<Row> rows = areaTop3Product.collect();

        List<AreaTop3Product> areaTop3Products = new ArrayList<>();

        for (Row row : rows) {

            String area = row.getString(0);
            String areaLevel = row.getString(1);
            long productId = row.getLong(2);
            int clickCount = Integer.parseInt(String.valueOf(row.get(3)));
            String cityInfos = row.getString(4);
            String productName = row.getString(5);
            String productStatus = row.getString(6);

            AreaTop3Product top3Product = new AreaTop3Product(
                    taskId,
                    area,
                    areaLevel,
                    Integer.valueOf((int) productId),
                    cityInfos,
                    clickCount,
                    productName,
                    productStatus);

            areaTop3Products.add(top3Product);

        }

        DAOFactory.getAreaTop3ProductDAO().insertBatch(areaTop3Products);
    }

    /**
     * 获取各区域排名前三的商品
     * @param sqlContext
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {

        String sql = "select" +
                        "area," +
                        "case" +
                            "when area='华北' or area='华东' then 'A级' "+
                            "when area='华南' or area='华中' then 'B级' "+
                            "when area='西北' or area='西南' then 'C级' "+
                            "else 'D级'"+
                        "end area_level,"+
                        "product_id," +
                        "click_count," +
                        "city_infos," +
                        "product_name," +
                        "product_status" +
                        "from (" +
                            "select" +
                                "area,product_id,click_count,city_infos,product_name,product_status, " +
                                "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
                            "from tmp_area_full_product_click_count " +
                            ") t " +
                    "where rank<=3 ";

        DataFrame df = sqlContext.sql(sql);

        return df.javaRDD();
    }

    /**
     * 生成区域商品点击次数临时表（包含了商品的完整信息）
     * @param sqlContext
     */
    private static void generateTmpAreaFullProductClickCountTable(SQLContext sqlContext) {

        String sql = " select " +
                "tapcc.area," +
                "tapcc.product_id," +
                "tapcc.click_count," +
                "tapcc.city_infos," +
                "pi.product_name," +
                "if(get_json_object(pi.extend_info,'product_status')='0','自营商品','第三方商品') product_status " +
                "from tmp_area_product_click_count tapcc " +
                "left join product_info pi on tapcc.product_id=pi.product_id ";


        /**
         * 随机key 与 扩容解决数据倾斜问题
         *
         */
//        JavaRDD<Row> productInfoRDD = sqlContext.sql("select * from product_info").javaRDD();
//        JavaRDD<Row> flattedRDD = productInfoRDD.flatMap(new FlatMapFunction<Row, Row>() {
//            @Override
//            public Iterable<Row> call(Row row) throws Exception {
//                List<Row> list = new ArrayList<>();
//
//                for (int i = 0; i < 10; i++) {
//                    long prodictId = row.getLong(0);
//                    String _productId = i + Constants.SPLIT_SYMBAL_UNDERLINE_BAR + prodictId;
//                    Row _row = RowFactory.create(_productId, row.get(1), row.get(2));
//                    list.add(_row);
//                }
//                return list;
//            }
//        });
//
//        StructType _schema = DataTypes.createStructType(Arrays.asList(
//                DataTypes.createStructField("product_id", DataTypes.StringType, true),
//                DataTypes.createStructField("product_name", DataTypes.StringType, true),
//                DataTypes.createStructField("product_status", DataTypes.StringType, true)
//        ));
//
//        DataFrame _df = sqlContext.createDataFrame(flattedRDD, _schema);
//        _df.registerTempTable("tmp_product_info");
//
//        String sql2 = " select " +
//                "tapcc.area," +
//                "remove_random_prefix(tapcc.product_id) product_id," +
//                "tapcc.click_count," +
//                "tapcc.city_infos," +
//                "pi.product_name," +
//                "if(get_json_object(pi.extend_info,'product_status')=0,'自营商品','第三方商品') product_status " +
//                "from ( " +
//                    "select " +
//                        "area," +
//                        "random_prefix(product_id,10) product_id," +
//                        "click_count," +
//                        "city_infos " +
//                    "from tmp_area_product_click_count " +
//                ") tapcc " +
//                "left join tmp_product_info pi on tapcc.product_id=pi.product_id ";

        DataFrame df = sqlContext.sql(sql);

        df.registerTempTable("tmp_area_full_product_click_count");
    }

    /**
     *
     * @param sqlContext
     */
    private static void generateTmpAreaProductClickCountTable(SQLContext sqlContext) {
        // 计算各区域商品点击次数
        // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
        StringBuffer sqlStr = new StringBuffer();
        sqlStr.append(" select area,proeuct_id,count(0) click_count,");
        sqlStr.append(" group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos ");
        sqlStr.append(" from tmp_click_product_basic ");
        sqlStr.append(" group by area,product_id");

        /**
         * 双重 group by 解决数据倾斜问题
         */
//        String sql ="select " +
//                        "product_id_area, " +
//                        "count(click_count) click_count, " +
//                        "group_concat_distinct(city_infos) city_infos " +
//                    "from ( " +
//                        "select" +
//                            "remove_random_prefix(product_id_area) product_id_area, " +
//                            "click_count, " +
//                            "city_infos " +
//                            "from (" +
//                                "select " +
//                                    "product_id_area, " +
//                                    "product_id_area random_key, " +
//                                    "count(0) click_count, " +
//                                    "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
//                                "from ( " +
//                                    "select" +
//                                        "random_prefix(concat_long_string(product_id,area,':'),10) product_id_area," +
//                                        "city_id," +
//                                        "city_name" +
//                                    "from tmp_click_product_basic" +
//                                ") t1 " +
//                                "group by product_id_area " +
//                        ") t2" +
//                    ") t3 " +
//                    "group by product_id_area";

        DataFrame df = sqlContext.sql(sqlStr.toString());

        df.registerTempTable("tmp_area_product_click_count");
    }

    /**
     * 生成点击商品临时表
     * @param sqlContext
     * @param clickActionRDD
     * @param cityInfoRDD
     */
    private static void generateTmpClickProductBasicTable(SQLContext sqlContext, JavaPairRDD<Long, Row> clickActionRDD, JavaPairRDD<Long, Row> cityInfoRDD) {

        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = clickActionRDD.join(cityInfoRDD);

        JavaRDD<Row> mappedRDD = joinedRDD.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
                Long cityId = tuple._1;
                Row clickAction = tuple._2._1;
                Row cityInfo = tuple._2._2;

                long productId = clickAction.getLong(1);
                String cityName = cityInfo.getString(1);
                String area = cityInfo.getString(2);

                return RowFactory.create(cityId, cityName, area, productId);
            }
        });

        // 基于JavaRDD<Row>格式，转换为DataFrame
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        StructType schema = DataTypes.createStructType(structFields);

        DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);

        // 将DF注册为临时表
        df.registerTempTable("tmp_click_product_basic");

    }


    /**
     * 从MySQL中获取城市信息
     *
     * @param sqlContext
     * @return
     */
    private static JavaPairRDD<Long, Row> getCityId2CityInfoRDD(SQLContext sqlContext) {

        // 构建MySQL连接配置信息
        String url = null;
        String user = null;
        String password = null;
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        } else {
            // TODO 非本地数据源配置
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        }

        Map<String, String> options = new HashMap<>();
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", user);
        options.put("password", password);

        // 通过sqlContext去从MySQL中查询数据
        DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();

        return cityInfoDF.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(Long.valueOf(String.valueOf(row.get(0))), row);
            }
        });
    }

    /**
     * 获取指定日期范围内的点击行为数据
     *
     * @param sqlContext
     * @param startDate
     * @param endDate
     * @return
     */
    private static JavaPairRDD<Long, Row> getCityId2ClickActionRDDByDate(SQLContext sqlContext, String startDate, String endDate) {

        // 从 user_visit_action 中查询数据

        StringBuffer sqlStr = new StringBuffer();
        sqlStr.append(" select city_id,click_product_id product_id ");
        sqlStr.append(" from user_visit_action ");
        sqlStr.append(" where click_product_id is not null ");
        sqlStr.append(" and click_product_id != 'NULL' ");
        sqlStr.append(" and click_product_id != 'null' ");
        sqlStr.append(" and date >= '").append(startDate).append("'");
        sqlStr.append(" and date <= '").append(endDate).append("'");

        DataFrame dataFrame = sqlContext.sql(sqlStr.toString());

        JavaRDD<Row> javaRDD = dataFrame.javaRDD();

        return javaRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });
    }

}
