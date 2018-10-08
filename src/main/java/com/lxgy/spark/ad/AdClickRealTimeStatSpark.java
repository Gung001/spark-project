package com.lxgy.spark.ad;

import com.lxgy.spark.conf.ConfigurationManager;
import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.dao.impl.DAOFactory;
import com.lxgy.spark.domain.AdUserClickCount;
import com.lxgy.spark.jdbc.JDBCHelper;
import com.lxgy.spark.utils.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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
                .setMaster("local[2]");

        // 每隔5分钟收集最近5秒内的数据源
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

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

        // 一条一条的实时日志
        // timestamp province city userId adid
        // 某个时间点 某个省份 某个城市 某个用户 某个广告

        // 计算出每5秒内的数据中，每天每个用户广告的点击量
        // 将日志的格式处理成<yyyyMMdd_userId_adId,1L>格式
        JavaPairDStream<String, Long> dailyUserAdClickDStream = adRealTimeLogDStream.mapToPair(
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


        jssc.awaitTermination();
        jssc.close();
    }

}
