package com.lxgy.spark.ad;

import com.google.common.base.Optional;
import com.lxgy.spark.conf.ConfigurationManager;
import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.dao.impl.DAOFactory;
import com.lxgy.spark.domain.*;
import com.lxgy.spark.utils.DateUtils;
import com.lxgy.spark.utils.SparkUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * @author Gryant
 */
public class AdClickRealTimeStatSpark {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_AD)
                .setMaster("local[2]")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.streaming.receiver.writeAheadLog.enable", "true")
//                .set("spark.streaming.blockInterval", "50")
                ;

        // 每隔5分钟收集最近5秒内的数据源
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 高可用性：存入文件系统一旦内存中数据丢失可以从文件系统中读取数据，不需要重新计算
//        jssc.checkpoint("hdfs://");

        // 构建spark streaming 上下文之后，需要启动/等待执行结束/关闭
        jssc.start();

        // 构建Kafka参数map，重要是存放你要连接的Kafka集群的地址
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(Constants.SPLIT_SYMBAL_COMMA);
        Set<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }

        // 基于Kafka direct api 模式，构建出了针对Kafka集群中指定topic的数据DStream
        // 两个值，value1，value2；value1没有什么特殊的意义，value2中包含了Kafka topic 中的一条条实时数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        // 刚刚接收到原始的用户点击行为日志后，剔除掉和名单中的用户点击的数据
        final JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlackList(adRealTimeLogDStream);

        // 重复区
//        filteredAdRealTimeLogDStream.repartition(1000);

        // 生成动态的黑名单
        generateDynamicBlackList(filteredAdRealTimeLogDStream);

        // 业务功能一：实时统计各省市广告点击量
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);

        // 业务功能二：实时统计每天每个省份top3热门广告
        calculateProvinceTop3Ad(adRealTimeStatDStream);

        // 业务功能三：实时统计每天每个广告在最近一小时滑动窗口内的点击趋势
        calculateAdClickCountByWindow(adRealTimeLogDStream);

        jssc.awaitTermination();
        jssc.close();
    }

    private void testDriverHA(){

        final String checkpointDirectory = "hdfs://";

        JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
            @Override
            public JavaStreamingContext create() {


                SparkConf conf = new SparkConf()
                        .setAppName(Constants.SPARK_APP_NAME_AD)
                        .setMaster("local[2]")
                        .set("spark.streaming.receiver.writeAheadLog.enable", "true")
                        ;

                // 每隔5分钟收集最近5秒内的数据源
                JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

                // 高可用性：存入文件系统一旦内存中数据丢失可以从文件系统中读取数据，不需要重新计算
//        jssc.checkpoint("hdfs://");

                // 构建spark streaming 上下文之后，需要启动/等待执行结束/关闭
                jssc.start();

                // 构建Kafka参数map，重要是存放你要连接的Kafka集群的地址
                Map<String, String> kafkaParams = new HashMap<>();
                kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
                        ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

                // 构建topic set
                String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
                String[] kafkaTopicsSplited = kafkaTopics.split(Constants.SPLIT_SYMBAL_COMMA);
                Set<String> topics = new HashSet<>();
                for (String kafkaTopic : kafkaTopicsSplited) {
                    topics.add(kafkaTopic);
                }

                // 基于Kafka direct api 模式，构建出了针对Kafka集群中指定topic的数据DStream
                // 两个值，value1，value2；value1没有什么特殊的意义，value2中包含了Kafka topic 中的一条条实时数据
                JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topics
                );

                // 刚刚接收到原始的用户点击行为日志后，剔除掉和名单中的用户点击的数据
                final JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlackList(adRealTimeLogDStream);

                // 生成动态的黑名单
                generateDynamicBlackList(filteredAdRealTimeLogDStream);

                // 业务功能一：实时统计各省市广告点击量
                JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);

                // 业务功能二：实时统计每天每个省份top3热门广告
                calculateProvinceTop3Ad(adRealTimeStatDStream);

                // 业务功能三：实时统计每天每个广告在最近一小时滑动窗口内的点击趋势
                calculateAdClickCountByWindow(adRealTimeLogDStream);

                return jssc;
            }
        };

        JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointDirectory, contextFactory);
        context.start();
        context.awaitTermination();

//        spark-submit
//                --deploy-mode cluster
//                --supervise
//
    }


    /**
     *
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {

        JavaPairDStream<String,Long> pairDStream = adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {

                // timestamp province city userId adId
                String[] logSplited = tuple._2.split(Constants.SPLIT_SYMBAL_SPACE);

                String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
                Long adId = Long.valueOf(logSplited[4]);

                return new Tuple2<>(timeMinute + Constants.SPLIT_SYMBAL_UNDERLINE_BAR + adId, 1L);
            }
        });

        // 过来的每个batch add ，都会被映射<yyyyMMddHHmm_adId,1L>
        // 每次出来一个新的batch，都要获取最近1小时内所有的batch
        // 然后根据key进行reduceByKey 操作，统计出来最近一个小时内的各分钟各广告的低级次数
        // 1小时滑动窗口函数
        // 点图/折线图
        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.minutes(60), Durations.seconds(10));

        // aggrRDD 每次都可以拿到，最近1小时内，各分钟（yyyyMMddHHmm）各广告的点击量
        // 各广告，最近1小时，各分钟的点击量
        aggrRDD.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {

                        List<AdClickTrend> adClickTrends = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();

                            String[] keySplited = tuple._1.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                            String dateKey = keySplited[0];
                            Integer adId = Integer.parseInt(keySplited[1]);
                            Integer clickCount = tuple._2.intValue();

                            String date = dateKey.substring(0, 8);
                            String hour = dateKey.substring(8, 10);
                            String minute = dateKey.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend(
                                    date, hour, minute, adId, clickCount
                            );

                            adClickTrends.add(adClickTrend);
                        }

                        DAOFactory.getAdClickTrendDAO().updateBatch(adClickTrends);
                    }
                });
                return null;
            }
        });
    }

    /**
     * 计算每天各省top3广告
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {

        // adRealTimeStatDStream 每个一个batch rdd 都代表了最新的全量的每天各省份各城市个广告的点击量
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {

                // <yyyyMMdd_province_adId,clickCount>
                JavaPairRDD<String, Long> dailyAdClickCount2ProvinceRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {

                        String[] oldKeySplit = tuple._1.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);

                        // 提取日期（yyyyMMdd）
                        String date = oldKeySplit[0];
                        String province = oldKeySplit[1];
                        Long adId = Long.valueOf(oldKeySplit[3]);
                        Long clickCount = tuple._2;

                        StringBuffer key = new StringBuffer();
                        key.append(date);
                        key.append(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                        key.append(province);
                        key.append(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                        key.append(adId);

                        return new Tuple2<>(key.toString(), clickCount);
                    }
                });

                JavaPairRDD<String, Long> dailyAdClickCounts2ProvinceRDD = dailyAdClickCount2ProvinceRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

                // 将 dailyAdClickCounts2ProvinceRDD 转换为 DataFrame 注册为一张临时表
                // 使用spark SQL，通过开窗函数，获取各省份top3 热门广告
                JavaRDD<Row> rowsRDD = dailyAdClickCounts2ProvinceRDD.map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> tuple) throws Exception {

                        String[] keys = tuple._1.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                        String dateKey = keys[0];
                        String province = keys[1];
                        Long adId = Long.parseLong(keys[2]);
                        Long clickCount = tuple._2;

                        // yyyy-MM-dd
                        String date = DateUtils.formatDate(DateUtils.formatDateKey(dateKey));

                        return RowFactory.create(date, province, adId, clickCount);
                    }
                });

                // 创建数据结构
                StructType schema = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("province", DataTypes.StringType, true),
                        DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                        DataTypes.createStructField("click_count", DataTypes.LongType, true)
                ));

                // 获取上下文对象
                SQLContext sqlContext = SparkUtils.getSQLContext(rdd.context());

                DataFrame dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);

                // 注册临时表
                dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_2_province");

                DataFrame df = sqlContext.sql(
                        "select" +
                                    "date," +
                                    "province," +
                                    "ad_id," +
                                    "click_count " +
                                "from ( " +
                                    "select" +
                                        "date," +
                                        "province," +
                                        "ad_id," +
                                        "click_count," +
                                        "row_number() over (partition by province order by click_count desc) rank " +
                                    "from tmp_daily_ad_click_count_2_province " +
                                ") t " +
                                "where t < 3 ");
                return df.javaRDD();
            }
        });

        // rowsDStream 每次刷新出来各个省份最热门的top3广告
        // 写入MySQL
        rowsDStream.foreachRDD(new Function<JavaRDD<Row>, Void>() {
            @Override
            public Void call(JavaRDD<Row> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {

                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<>();

                        while (iterator.hasNext()) {

                            Row row = iterator.next();

                            String date = row.getString(1);
                            String province = row.getString(2);
                            Integer adId = Integer.parseInt(String.valueOf(row.get(3)));
                            Integer clickCount = Integer.parseInt(String.valueOf(row.get(4)));

                            adProvinceTop3s.add(new AdProvinceTop3(date, province, adId, clickCount));
                        }

                        DAOFactory.getAdProvinceTop3DAO().updateBatch(adProvinceTop3s);
                    }
                });
                return null;
            }
        });

    }

    /**
     * 计算广告点击流量实时统计结果
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {

        JavaPairDStream<String,Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {

                // 从tuple中获取到每一条原始的实时日志
                String log = tuple._2;
                String[] logSplited = log.split(Constants.SPLIT_SYMBAL_SPACE);

                // 提取日期（yyyyMMdd）
                String timestamp = logSplited[0];
                Date date = new Date(Long.valueOf(timestamp));
                String dateKey = DateUtils.formatDateKey(date);
                String province = logSplited[1];
                String city = logSplited[2];
                Long adId = Long.valueOf(logSplited[4]);

                StringBuffer key = new StringBuffer();
                key.append(dateKey);
                key.append(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                key.append(province);
                key.append(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                key.append(city);
                key.append(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                key.append(adId);

                return new Tuple2<>(key.toString(), 1L);
            }
        });

        // 计算广告点击流量实时结果（yyyyMMdd_province_city_adid_clickCount）
        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey(
                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
                    @Override
                    public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {

                        // 举例来说：对于每个key都会调用一次这个方法
                        // values(1,1,1,1,1,1)

                        long clickCount = 0L;

                        // 如果之前存在key，那么在原有的基础上添加
                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        // values 代表batch rdd 中，每个key 对应的值
                        for (Long value : values) {
                            clickCount += value;
                        }

                        return Optional.of(clickCount);
                    }
                });

        // 结果在MySQL中存储一份
        aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> javaPairRDD) throws Exception {
                javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {


                        List<AdStat> adStats = new ArrayList<>();

                        while (iterator.hasNext()) {
                            // <yyyyMMdd_province_city_adId,clickCount>
                            Tuple2<String, Long> tuple = iterator.next();
                            Long clickCount = tuple._2;
                            String key = tuple._1;
                            String[] split = key.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                            String date = split[0];
                            String province = split[1];
                            String city = split[2];
                            Integer adId = Integer.parseInt(split[3]);

                            AdStat adStat = new AdStat(
                                    date, province, city, adId, clickCount.intValue()
                            );

                            adStats.add(adStat);

                        }

                        DAOFactory.getAdStatDAO().updateBatch(adStats);
                    }
                });
                return null;
            }
        });

        return aggregatedDStream;
    }

    /**
     * 动态生成黑名单
     * @param filteredAdRealTimeLogDStream
     */
    private static void generateDynamicBlackList(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {

        // 一条一条的实时日志
        // timestamp province city userId adid
        // 某个时间点 某个省份 某个城市 某个用户 某个广告

        // 计算出每5秒内的数据中，每天每个用户广告的点击量
        // 将日志的格式处理成<yyyyMMdd_userId_adId,1L>格式
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {

                        // 从tuple中获取到每一条原始的实时日志
                        String log = tuple._2;
                        String[] logSplited = log.split(Constants.SPLIT_SYMBAL_SPACE);

                        // 提取日期（yyyyMMdd）
                        String timestamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String dateKey = DateUtils.formatDateKey(date);

                        Long userId = Long.valueOf(logSplited[3]);
                        Long adId = Long.valueOf(logSplited[4]);

                        // 拼接key
                        StringBuffer str = new StringBuffer();
                        str.append(dateKey);
                        str.append(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                        str.append(userId);
                        str.append(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                        str.append(adId);

                        return new Tuple2<>(str.toString(), 1L);
                    }
        });

        // 针对处理后的日志格式，执行reduceByKey算子
        // （每个batch中）每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
        });

        // 源源不断的，每隔5s的batch中，当天每个用户对每支广告的点击次数
        // <yyyyMMdd_userId_adId,clickCount>
        dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> javaPairRDD) throws Exception {

                javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {

                        List<AdUserClickCount> adUserClickCounts = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();

                            String[] keySplited = tuple._1.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                            // yyyy-MM-dd
                            String date = DateUtils.formatDate(DateUtils.formatDateKey(keySplited[0]));
                            Integer userId = Integer.parseInt(keySplited[1]);
                            Integer adId = Integer.parseInt(keySplited[2]);
                            Integer clickCount = Integer.valueOf(String.valueOf(tuple._2));

                            adUserClickCounts.add(new AdUserClickCount(date, userId, adId, clickCount));
                        }

                        DAOFactory.getAdUserClickCountDAO().updateBatch(adUserClickCounts);
                    }
                });

                return null;
            }
        });

        // 现在我们在MySQL里面，已经有了累计的每天各用户对各广告的点击量
        // 遍历每个batch中的所有记录，这一天这个用户对这个广告点击了多少次
        // 从MySQL中查询，如果超过了100那么就判定用户时黑名单用户，那么写入MySQL
        JavaPairDStream<String, Long> blackListDStream = dailyUserAdClickCountDStream.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> tuple) throws Exception {

                String[] keySplited = tuple._1.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR);
                // yyyy-MM-dd
                String date = DateUtils.formatDate(DateUtils.formatDateKey(keySplited[0]));
                Integer userId = Integer.parseInt(keySplited[1]);
                Integer adId = Integer.parseInt(keySplited[2]);

                // 查询指定日期指定用户对某个广告的点击量
                int clickCount = DAOFactory.getAdUserClickCountDAO().findClickCountByMultiKey(date, userId, adId);

                if (clickCount >= 100) {
                    // 加入黑名单DStream
                    return true;
                }

                return false;
            }
        });

        // 黑名单入库 blackListDStream
        // 去重
        JavaDStream<Long> blackListUserIdDStream = blackListDStream.map(new Function<Tuple2<String, Long>, Long>() {
            @Override
            public Long call(Tuple2<String, Long> tuple) throws Exception {
                String key = tuple._1;
                Long userId = Long.valueOf(key.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR)[1]);
                return userId;
            }
        });

        JavaDStream<Long> distinctBlackListUserIdDstream = blackListUserIdDStream.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
            @Override
            public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                return rdd.distinct();
            }
        });

        // 黑名单入库操作
        distinctBlackListUserIdDstream.foreachRDD(new Function<JavaRDD<Long>, Void>() {
            @Override
            public Void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    @Override
                    public void call(Iterator<Long> iterator) throws Exception {
                        List<AdBlackList> adBlackLists = new ArrayList<>();

                        while (iterator.hasNext()) {

                            Long userId = iterator.next();

                            AdBlackList adBlackList = new AdBlackList(userId.intValue());
                            adBlackLists.add(adBlackList);
                        }

                        DAOFactory.getAdBlackListDAO().insertBatch(adBlackLists);
                    }
                });
                return null;
            }
        });
    }

    /**
     * 根据黑名单对原始数据进行过滤
     * @param adRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, String> filterByBlackList(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        return adRealTimeLogDStream.transformToPair(
                    new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                        @Override
                        public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {

                            // 查询库中所有的黑名单用户，转换为一个RDD
                            List<AdBlackList> adBlackLists = DAOFactory.getAdBlackListDAO().findAll();
                            List<Tuple2<Long, Boolean>> adBlackListTuple = new ArrayList<>();

                            for (AdBlackList adBlackList : adBlackLists) {
                                adBlackListTuple.add(new Tuple2<Long, Boolean>(adBlackList.getUserId().longValue(), true));
                            }

                            JavaSparkContext sc = new JavaSparkContext(rdd.context());
                            JavaPairRDD<Long, Boolean> adBlackListRDD = sc.parallelizePairs(adBlackListTuple);

                            // 将原始数据映射成为<userId,tuple2<string,string>>
                            JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(
                                    new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                                        @Override
                                        public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                                            String log = tuple._2;
                                            String[] logSplited = log.split(Constants.SPLIT_SYMBAL_SPACE);
                                            Long userId = Long.valueOf(logSplited[3]);
                                            return new Tuple2<>(userId, tuple);
                                        }
                                    });

                            // 将原始日志数据rdd与黑名单rdd，进行左外连接
                            JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(adBlackListRDD);

                            // 过滤掉黑名单用户
                            JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                                @Override
                                public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                    Optional<Boolean> optional = tuple._2._2;

                                    // 如果这个值存在&join到了某个黑名单用户-->剔除该数据
                                    if (optional.isPresent() && optional.get()) {
                                        return false;
                                    }
                                    return true;
                                }
                            });

                            // 得到结果RDD
                            JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                                @Override
                                public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                    return tuple._2._1;
                                }
                            });

                            return resultRDD;
                        }
                    });
    }

}
