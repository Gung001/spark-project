package com.lxgy.spark.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.lxgy.spark.conf.ConfigurationManager;
import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.dao.ISessionAggrStatDAO;
import com.lxgy.spark.dao.ISessionDetailDAO;
import com.lxgy.spark.dao.ISessionRandomExtractDAO;
import com.lxgy.spark.dao.ITaskDAO;
import com.lxgy.spark.dao.impl.DAOFactory;
import com.lxgy.spark.domain.*;
import com.lxgy.spark.mock.MockData;
import com.lxgy.spark.utils.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session分析Spark作业
 * <p>
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * <p>
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * <p>
 * 我们的spark作业如何接受用户创建的任务？
 * <p>
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 * <p>
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 * <p>
 * 这是spark本身提供的特性
 *
 * @author Gryant
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        args = new String[]{"1"};

        // 构建Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME)
                .setMaster("local")
                /**并行度*/
//				.set("spark.default.parallelism", "100")
                /** jvm 调优 ：调节cache操作内存占比 */
                .set("spark.storage.memoryFraction", "0.5")
                /**合并map端输出文件*/
                .set("spark.shuffle.consolidateFiles", "true")
                /**map端缓冲内存*/
                .set("spark.shuffle.file.buffer", "64")
                /**reduce端聚合内存占比*/
                .set("spark.shuffle.memoryFraction", "0.3")
                /**reduce 缓冲大小调节*/
                .set("spark.reducer.maxSizeInFlight", "24")
                /**shuffle文件拉去重试次数，默认三次*/
                .set("spark.shuffle.io.maxRetries", "60")
                /**shuffle文件每次拉取文件间隔时间，默认5s*/
                .set("spark.shuffle.io.retryWait", "60")
                /**更改默认的序列化方式*/
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{
                        CategorySortKey.class,
                        IntList.class
                });
        /**
         * registerKryoClasses
         * 为了使 KryoSerializer 达到最佳性能，需要注册我们自定义的类，比如：CategorySortKey
         * CategorySortKey 在进行shuffle的时候，进行网络传输，因此也是要求实现序列化的
         */

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 设置checkpoint机制，文件存储在HDFS上的路径
//        sc.checkpointFile("hdfs://")

        SQLContext sqlContext = getSQLContext(sc.sc());

        // 生成模拟测试数据
        mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 首先得查询出来指定的任务，并获取任务的查询参数
        Task task = taskDAO.findById(ParamUtils.getTaskIdFromArgs(args));
        if (task == null) {
            throw new RuntimeException("没有获取到指定的任务：" + JSON.toJSONString(args));
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 如果要进行session粒度的数据聚合，首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        // 获取sessionId映射的明细
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);

        /**
         * 持久化，就是对RDD调用persist()方法，并传入一个持久化级别
         * 持久化级别：
         * StorageLevel.MEMORY_ONLY() 纯内存，无序列化，可用cache()替代
         * StorageLevel.MEMORY_ONLY_SER()
         * StorageLevel.MEMORY_AND_DISK()
         * StorageLevel.MEMORY_AND_DISK_SER()
         * StorageLevel.DISK_ONLY()
         *
         * 如果内存充足，要使用双副本高可靠机制选择后缀带_2的策略，比如：
         * StorageLevel.MEMORY_ONLY_2()
         */
        sessionId2ActionRDD = sessionId2ActionRDD.persist(StorageLevel.MEMORY_ONLY());

        // RDD checkpoint
//        sessionId2ActionRDD.checkpoint();


        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggregateBySession(sqlContext, sessionId2ActionRDD);
        System.out.println("====总共数据：" + sessionId2AggrInfoRDD.count());
        for (Tuple2<String, String> tuple : sessionId2AggrInfoRDD.take(1)) {
            System.out.println("==数据格式：" + tuple._2);
        }

        // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
//        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
//                filterSession(sessionId2AggrInfoRDD, taskParam);
//        System.out.println("====过滤后的数据："+filteredSessionid2AggrInfoRDD.count());

        // 重构此部分代码（同时进行过滤和统计）
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
                filterSessionAndAggrStat(sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        /**
         * 持久化
         */
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

        // 获取符合条件的session访问明细
        JavaPairRDD<String, Row> sessionId2detailRDD = getSession2DetailRDD(filteredSessionid2AggrInfoRDD, sessionId2ActionRDD);

        /**
         * 持久化
         */
        sessionId2detailRDD = sessionId2detailRDD.persist(StorageLevel.MEMORY_ONLY());


        /***
         * 对于Accumulator分布式累加计算的变量的使用：
         * 从Accumulator中获取数据插入数据库的时候一定要，一定要是在某一个action操作以后再进行
         * 如果没有action的话整个程序是不会运行的
         *
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
         */

//        // 触发计算
//        filteredSessionid2AggrInfoRDD.count();
        /**
         * 特别说明
         * 要将上一个功能的session聚合统计数据获取到，就必须再一个action触发job之后才能从Accumulator中获取数据
         * 否则无法获取，因为没有job执行，Accumulator的值为空
         *
         * 将随机抽取的功能的实现代码放在session聚合统计功能的最终计算和写库之前，因为随机抽取功能中，有一个groupByKey算子
         * 是action操作，会促发job
         */
        randomExtractSession(sc, task.getTaskId(), filteredSessionid2AggrInfoRDD, sessionId2detailRDD);

        // 计算出各个范围的session占比，并写入Mysql
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());

        /**
         * session聚合统计（统计出访问时长和步长，各个区间的session数量占总session数量的比例）
         *
         * 如果不进行重构，直接来实现，思路：
         * 1、actionRDD，映射成<sessionid,Row>的格式
         * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
         * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
         * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
         * 5、将最后计算出来的结果，写入MySQL对应的表中
         *
         * 普通实现思路的问题：
         * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
         * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
         * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
         *
         * 重构实现思路：
         * 1、不要去生成任何新的RDD（处理上亿的数据）
         * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
         * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
         * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
         * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
         * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
         * 		半个小时，或者数个小时
         *
         * 开发Spark大型复杂项目的一些经验准则：
         * 1、尽量少生成RDD
         * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
         * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
         * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
         * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
         * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
         * 4、无论做什么功能，性能第一
         * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
         * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
         *
         * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
         * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
         * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
         * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
         * 		此时，对于用户体验，简直就是一场灾难
         *
         * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
         *
         * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
         * 		如果采用第二种方案，那么其实就是性能优先
         *
         * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
         * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
         * 		积累了，处理各种问题的经验
         */

        // 获取top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskId(), sessionId2detailRDD);

        // 获取top10活跃session
        getTop10Session(task.getTaskId(), sc, sessionId2detailRDD, top10CategoryList);


        // 关闭Spark上下文
        sc.close();
    }

    /**
     * 获取top10活跃session
     *
     * @param taskId
     * @param sc
     * @param sessionId2detailRDD
     * @param top10CategoryList
     */
    private static void getTop10Session(final Integer taskId,
                                        JavaSparkContext sc,
                                        JavaPairRDD<String, Row> sessionId2detailRDD,
                                        List<Tuple2<CategorySortKey, String>> top10CategoryList) {

        /**
         * 第一步：根据top10热门品类的id，生成一份RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();

        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            Long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(tuple._2, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryId, categoryId));
        }

        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);


        /**
         * 第二步：获取到top10热门品类，被各个session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionId2detailsRDD = sessionId2detailRDD.groupByKey();
        JavaPairRDD<Long, String> categoryId2sessionCountRDD = sessionId2detailsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();
                        Map<Long, Long> categoryCountMap = new HashMap<>();

                        // 计算该session对每个品类的点击次数
                        while (iterator.hasNext()) {
                            Row row = iterator.next();

                            if (row.get(6) != null) {
                                Long categoryId = row.getLong(6);

                                Long categoryCount = categoryCountMap.get(categoryId);
                                if (categoryCount == null) {
                                    categoryCount = 0L;
                                }

                                categoryCount++;

                                categoryCountMap.put(categoryId, categoryCount);
                            }
                        }

                        // 返回结果，格式：<categoryId,sessionId,count>
                        List<Tuple2<Long, String>> list = new ArrayList<>();

                        for (Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {

                            StringBuffer value = new StringBuffer();
                            value.append(sessionId);
                            value.append(Constants.SPLIT_SYMBAL_COMMA);
                            value.append(categoryCountEntry.getValue());
                            list.add(new Tuple2<Long, String>(categoryCountEntry.getKey(), value.toString()));
                        }

                        return list;
                    }
                });

        JavaPairRDD<Long, String> top10CategorySessionCountRDD =
                top10CategoryIdRDD.join(categoryId2sessionCountRDD)
//                        .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
//                            @Override
//                            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
//                                return new Tuple2<>(tuple._1, tuple._2._2);
//                            }
//                        });
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<Long, String>>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(Iterator<Tuple2<Long, Tuple2<Long, String>>> iterator) throws Exception {

                        List<Tuple2<Long, String>> list = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<Long, Tuple2<Long, String>> tuple = iterator.next();
                            list.add(new Tuple2<>(tuple._1, tuple._2._2));
                        }

                        return list;
                    }
                });

        /**
         * 第三步：分组取topN算法实现，获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = (JavaPairRDD<Long, Iterable<String>>) top10CategorySessionCountRDD.groupByKey();
        top10CategorySessionCountsRDD.foreach(new VoidFunction<Tuple2<Long, Iterable<String>>>() {
            @Override
            public void call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                System.out.println("==========groupByKey:" + tuple._1 + "_" + tuple._2);
            }
        });

        // TODO 没有执行（暂未找到原因）
        JavaPairRDD<String, String> top10CategorySessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {

                        // 定义topN排序数组
                        String[] top10Sessions = new String[10];

                        Long categoryId = tuple._1;
                        Iterator<String> iterator = tuple._2.iterator();
                        System.out.println("----------flatMapToPair:" + categoryId + "," + iterator.hasNext());
                        while (iterator.hasNext()) {

                            String sessionCount = iterator.next();
                            String[] split = sessionCount.split(Constants.SPLIT_SYMBAL_COMMA);
                            Long count = Long.parseLong(split[1]);

                            // 遍历排序数组
                            for (int i = 0; i < top10Sessions.length; i++) {
                                // 如果i位没有数据，直接将sessionCount赋予其
                                if (top10Sessions[i] == null) {
                                    top10Sessions[i] = sessionCount;
                                    break;
                                } else {

                                    Long _count = Long.valueOf(top10Sessions[i].split(Constants.SPLIT_SYMBAL_COMMA)[1]);
                                    // 如果sessionCount 比i位的sessionCount要大
                                    if (count > _count) {
                                        // 从排位数组最后一位开始到i位，所有数据往后挪一位
                                        for (int j = (top10Sessions.length - 1); j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }
                                        // 将sessionCount赋值给i位
                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }

                                    // 如果小，继续外层循环
                                }
                            }
                        }

                        // 组装返回的数据&写入数据库
                        List<Tuple2<String, String>> list = new ArrayList<>();
                        for (String sessionCount : top10Sessions) {

                            if (StringUtils.isEmpty(sessionCount)) {
                                continue;
                            }

                            String sessionId = sessionCount.split(Constants.SPLIT_SYMBAL_COMMA)[0];
                            Integer count = Integer.valueOf(sessionCount.split(Constants.SPLIT_SYMBAL_COMMA)[1]);

                            // 写入MySQL
                            Top10CategorySession top10CategorySession = new Top10CategorySession();
                            top10CategorySession.setTaskId(taskId);
                            top10CategorySession.setCategoryId(categoryId.intValue());
                            top10CategorySession.setSessionId(sessionId);
                            top10CategorySession.setClickCount(count);
                            DAOFactory.getTop10CategorySessionDAO().insert(top10CategorySession);

                            // 返回数据
                            list.add(new Tuple2<String, String>(sessionId, sessionId));
                        }

                        return list;
                    }
                }
        );

        /**
         * 第四步：获取活跃top10Session明细数据并写入MySQL
         */
//        JavaPairRDD<String, Tuple2<String, Row>> extractDetailRDD = top10CategorySessionRDD.join(sessionId2detailRDD);
//        extractDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
//
//                Row row = tuple2._2._2;
//
//                SessionDetail sessionDetail = new SessionDetail();
//                sessionDetail.setTaskId(taskId);
//                sessionDetail.setUserId(row.getLong(1));
//                sessionDetail.setSessionId(row.getString(2));
//                sessionDetail.setPageId(row.getLong(3));
//                sessionDetail.setActionTime(row.getString(4));
//                sessionDetail.setSearchKeyword(row.getString(5));
//                sessionDetail.setClickCategoryId(row.getLong(6));
//                sessionDetail.setClickProductId(row.getLong(7));
//                sessionDetail.setOrderCategoryIds(row.getString(8));
//                sessionDetail.setOrderProductIds(row.getString(9));
//                sessionDetail.setPayCategoryIds(row.getString(10));
//                sessionDetail.setPayProductIds(row.getString(11));
//
//                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
//                sessionDetailDAO.insert(sessionDetail);
//
//            }
//        });

    }

    /**
     * 获取top10热门品类
     *
     * @param taskId
     * @param sessionId2detailRDD
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(Integer taskId, JavaPairRDD<String, Row> sessionId2detailRDD) {

        /**
         * 第一步：获取符合条件的session访问过的所有品类
         */

        // 获取session访问过的所有session品类id
        JavaPairRDD<Long, Long> categoryIdRDD = sessionId2detailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {

                        Row row = tuple._2;
                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        // 获取点击品类id
                        Long clickCategoryId = row.getLong(6);
                        if (clickCategoryId != null) {
                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                        }

                        // 获取下单品类id
                        addCategoryId(row, 8, list);


                        // 获取支付品类id
                        addCategoryId(row, 10, list);

                        return list;
                    }
                });

        /**
         * 去重
         */
        categoryIdRDD = categoryIdRDD.distinct();

        /**
         * 第二步：计算各品类的点击/下单和支付的次数
         *
         * 访问明细中的三种行为：点击/支付/下单
         * 分别计算个品类点击/支付和下单的次数，先对访问明细数据进行过滤
         * 分别过滤出点击/支付和下单行为，然后通过map/reduceByKey等算子来进行计算
         */

        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryCount = getClickCategoryCount(sessionId2detailRDD);

        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryCount = categoryCount(sessionId2detailRDD, 8);

        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryCount = categoryCount(sessionId2detailRDD, 10);


        /**
         * 第三步：join个品类与它点击/下单和支付的次数
         *
         * categoryIdRDD中是包含了所有的符合条件的session（的品类id）
         * 第二部计算出来的各自（点击/支付/下单）次数可能不包含所有的品类
         *
         * so 这里不能使用join操作，而要使用leftOuterJoin
         * 换句话说，就是如果categoryIdRDD不能join到自己的某个数据，那么该品类id还是需要保留下来的（值为0）
         */
        JavaPairRDD<Long, String> category2CountRDD = joinCategoryAndData(categoryIdRDD, clickCategoryCount, orderCategoryCount, payCategoryCount);

        /**
         * 第四步：自定义二次排序key(必须序列化)
         */

        /**
         * 第五步：将数据映射成为<CategorySortKey,info>格式的RDD进行二次排序(降序)
         */
        JavaPairRDD<CategorySortKey, String> sortKey2CountRDD = category2CountRDD
//                .mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
//                    @Override
//                    public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
//
//                        String countInfo = tuple._2;
//                        Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_CLICK_CATEGORY));
//                        Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_ORDER_CATEGORY));
//                        Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_PAY_CATEGORY));
//
//                        CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);
//
//                        return new Tuple2<>(categorySortKey, countInfo);
//                    }
//                });
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, String>>, CategorySortKey, String>() {
                    @Override
                    public Iterable<Tuple2<CategorySortKey, String>> call(Iterator<Tuple2<Long, String>> iterator) throws Exception {
                        List<Tuple2<CategorySortKey, String>> list = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<Long, String> tuple = iterator.next();
                            String countInfo = tuple._2;
                            Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_CLICK_CATEGORY));
                            Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_ORDER_CATEGORY));
                            Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_PAY_CATEGORY));

                            CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);

                            list.add(new Tuple2<>(categorySortKey, countInfo));
                        }

                        return list;
                    }
                });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2CountRDD.sortByKey(false);

        /**
         * 第六步：使用take(10)取出top10热门品类， 并写入MySQL
         */
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            String countInfo = tuple._2;

            Integer categoryId = Integer.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_CATEGORY_ID));
            Integer clickCount = Integer.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_CLICK_CATEGORY));
            Integer orderCount = Integer.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_ORDER_CATEGORY));
            Integer payCount = Integer.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_COUNT_PAY_CATEGORY));

            Top10Category category = new Top10Category(taskId, categoryId, clickCount, orderCount, payCount);
            DAOFactory.getTop10CategoryDAO().insert(category);
        }

        return top10CategoryList;

    }

    /**
     * 获取符合条件的session访问明细
     *
     * @param sessionid2AggrInfoRDD
     * @param sessionId2ActionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSession2DetailRDD(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2ActionRDD) {
        return sessionid2AggrInfoRDD
                .join(sessionId2ActionRDD)
//                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
//                    @Override
//                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//                        return new Tuple2<>(tuple._1, tuple._2._2);
//                    }
//                });
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>, String, Row>() {
                    @Override
                    public Iterable<Tuple2<String, Row>> call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
                        List<Tuple2<String, Row>> list = new ArrayList<>();


                        while (iterator.hasNext()) {
                            Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
                            list.add(new Tuple2<>(tuple._1, tuple._2._2));
                        }

                        return list;
                    }
                });
    }

    /**
     * 连接品类和数据RDD
     *
     * @param categoryIdRDD
     * @param clickCategoryCount
     * @param orderCategoryCount
     * @param payCategoryCount
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryIdRDD,
            JavaPairRDD<Long, Long> clickCategoryCount,
            JavaPairRDD<Long, Long> orderCategoryCount,
            JavaPairRDD<Long, Long> payCategoryCount) {

        // 如果使用left Outer Join就可能出现右边的RDD中没有值，所有会用Optional类型
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryIdRDD
                .leftOuterJoin(clickCategoryCount);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD
//                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
//                    @Override
//                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
//                        Long categoryId = tuple._1;
//                        Optional<Long> optional = tuple._2._2;
//                        Long count = 0L;
//                        if (optional.isPresent()) {
//                            count = optional.get();
//                        }
//
//                        StringBuffer sb = new StringBuffer();
//                        sb.append(Constants.FIELD_COUNT_CATEGORY_ID).append(Constants.SYMBAL_EQUALS_SIGN).append(categoryId).append(Constants.SYMBAL_VERTICAL_BAR);
//                        sb.append(Constants.FIELD_COUNT_CLICK_CATEGORY).append(Constants.SYMBAL_EQUALS_SIGN).append(count).append(Constants.SYMBAL_VERTICAL_BAR);
//
//                        return new Tuple2<>(categoryId, sb.toString());
//                    }
//                });
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<Long, Optional<Long>>>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(Iterator<Tuple2<Long, Tuple2<Long, Optional<Long>>>> iterator) throws Exception {

                        List<Tuple2<Long, String>> list = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple = iterator.next();
                            Long categoryId = tuple._1;
                            Optional<Long> optional = tuple._2._2;
                            Long count = 0L;
                            if (optional.isPresent()) {
                                count = optional.get();
                            }

                            StringBuffer sb = new StringBuffer();
                            sb.append(Constants.FIELD_COUNT_CATEGORY_ID).append(Constants.SYMBAL_EQUALS_SIGN).append(categoryId).append(Constants.SYMBAL_VERTICAL_BAR);
                            sb.append(Constants.FIELD_COUNT_CLICK_CATEGORY).append(Constants.SYMBAL_EQUALS_SIGN).append(count).append(Constants.SYMBAL_VERTICAL_BAR);

                            list.add(new Tuple2<>(categoryId, sb.toString()));

                        }

                        return list;
                    }
                });

        // join 下单品类次数
        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryCount)
//                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
//                    @Override
//                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
//
//                        Long categoryId = tuple._1;
//                        String value = tuple._2._1;
//                        Optional<Long> optional = tuple._2._2;
//                        Long orderCount = 0L;
//                        if (optional.isPresent()) {
//                            orderCount = optional.get();
//                        }
//
//                        StringBuffer sb = new StringBuffer(value);
//                        sb.append(Constants.FIELD_COUNT_ORDER_CATEGORY).append(Constants.SYMBAL_EQUALS_SIGN).append(orderCount).append(Constants.SYMBAL_VERTICAL_BAR);
//
//                        return new Tuple2<>(categoryId, sb.toString());
//                    }
//                });
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<String, Optional<Long>>>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(Iterator<Tuple2<Long, Tuple2<String, Optional<Long>>>> iterator) throws Exception {

                        List<Tuple2<Long, String>> list = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple = iterator.next();
                            Long categoryId = tuple._1;
                            String value = tuple._2._1;
                            Optional<Long> optional = tuple._2._2;
                            Long orderCount = 0L;
                            if (optional.isPresent()) {
                                orderCount = optional.get();
                            }

                            StringBuffer sb = new StringBuffer(value);
                            sb.append(Constants.FIELD_COUNT_ORDER_CATEGORY).append(Constants.SYMBAL_EQUALS_SIGN).append(orderCount).append(Constants.SYMBAL_VERTICAL_BAR);

                            list.add(new Tuple2<>(categoryId, sb.toString()));
                        }

                        return list;
                    }
                });

        // join 支付品类次数
        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryCount)
//                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
//                    @Override
//                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
//
//                        Long categoryId = tuple._1;
//                        String value = tuple._2._1;
//                        Optional<Long> optional = tuple._2._2;
//                        Long payCount = 0L;
//                        if (optional.isPresent()) {
//                            payCount = optional.get();
//                        }
//
//                        StringBuffer sb = new StringBuffer(value);
//                        sb.append(Constants.FIELD_COUNT_PAY_CATEGORY).append(Constants.SYMBAL_EQUALS_SIGN).append(payCount).append(Constants.SYMBAL_VERTICAL_BAR);
//
//                        return new Tuple2<>(categoryId, sb.toString());
//                    }
//                });
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<String, Optional<Long>>>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(Iterator<Tuple2<Long, Tuple2<String, Optional<Long>>>> iterator) throws Exception {

                        List<Tuple2<Long, String>> list = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple = iterator.next();

                            Long categoryId = tuple._1;
                            String value = tuple._2._1;
                            Optional<Long> optional = tuple._2._2;
                            Long payCount = 0L;
                            if (optional.isPresent()) {
                                payCount = optional.get();
                            }

                            StringBuffer sb = new StringBuffer(value);
                            sb.append(Constants.FIELD_COUNT_PAY_CATEGORY).append(Constants.SYMBAL_EQUALS_SIGN).append(payCount).append(Constants.SYMBAL_VERTICAL_BAR);

                            list.add(new Tuple2<>(categoryId, sb.toString()));
                        }

                        return list;
                    }
                });

        return tmpMapRDD;
    }

    /**
     * 获取品类点击次数
     *
     * @param sessionId2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryCount(JavaPairRDD<String, Row> sessionId2detailRDD) {

        /**
         * 特别说明：
         * 这是对完整数据的过滤，过滤后得到点击行为的数据，其只占总数据的一小部分
         * 所以过滤后的RDD每个partition的数据量可能会很不均匀，而且数据量肯定会变少
         *
         * so 可以用一下 coalesce
         */
        JavaPairRDD<String, Row> clickActionRDD = sessionId2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.get(6) != null ? true : false;
                    }
                }).coalesce(100);

        /**
         * coalesce 操作说明：
         * local模式下不用取设置分区和并行度
         * local模式本身就是进程内模拟的集群来执行，本身性能就很高而且对并行度和partition数量都有一定的优化
         */


        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD
//                .mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
//                    @Override
//                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
//                        long clickCategoryId = tuple._2.getLong(6);
//                        return new Tuple2<>(clickCategoryId, 1L);
//                    }
//                });
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Row>>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Iterator<Tuple2<String, Row>> iterator) throws Exception {

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Row> tuple = iterator.next();
                            long clickCategoryId = tuple._2.getLong(6);
                            list.add(new Tuple2<>(clickCategoryId, 1L));
                        }

                        return list;
                    }
                });


        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return clickCategoryId2CountRDD;
    }

    /**
     * 统计品类（下单/支付）次数
     *
     * @param sessionId2detailRDD
     * @param i
     * @return
     */
    private static JavaPairRDD<Long, Long> categoryCount(
            JavaPairRDD<String, Row> sessionId2detailRDD, final int i) {

        // 过滤掉没有分类的数据
        JavaPairRDD<String, Row> payActionRDD = sessionId2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        return StringUtils.isNotEmpty(tuple._2.getString(i)) ? true : false;
                    }
                });

        // 得到所有的品类
        JavaPairRDD<Long, Long> categoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        addCategoryId(tuple._2, i, list);

                        return list;
                    }
                }
        );

        JavaPairRDD<Long, Long> categoryId2CountRDD = categoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return categoryId2CountRDD;
    }

    /**
     * 添加分类id
     *
     * @param row
     * @param i
     * @param list
     */
    private static void addCategoryId(Row row, int i, List<Tuple2<Long, Long>> list) {

        String categoryIds = row.getString(i);
        if (StringUtils.isEmpty(categoryIds)) {
            return;
        }
        String[] categoryIdArr = categoryIds.split(Constants.SPLIT_SYMBAL_COMMA);
        for (String categoryId : categoryIdArr) {
            if (StringUtils.isEmpty(categoryId)) {
                continue;
            }

            Long oci = Long.parseLong(categoryId);
            list.add(new Tuple2<Long, Long>(oci, oci));
        }
    }

    /**
     * 获取sessionId到访问行为数据映射的RDD
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
//        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//            @Override
//            public Tuple2<String, Row> call(Row row) throws Exception {
//                return new Tuple2<String, Row>(row.getString(2), row);
//            }
//        });
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                List<Tuple2<String, Row>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    list.add(new Tuple2<String, Row>(row.getString(2), row));
                }
                return list;
            }
        });
    }

    /**
     * 随机抽取 session
     *
     * @param taskId
     * @param sessionid2AggrInfoRDD
     * @param sessionId2ActionRDD
     */
    private static void randomExtractSession(
            JavaSparkContext sc,
            final Integer taskId,
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2ActionRDD) {

        // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>RDD
        JavaPairRDD<String, String> time2aggrInfoRDD = sessionid2AggrInfoRDD
//                .mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
//
//                    @Override
//                    public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
//
//                        String aggrInfo = tuple2._2;
//                        String startTime = StringUtils.getFieldFromConcatString(aggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_START_TIME);
//                        String dateHour = DateUtils.getDateHour(startTime);
//
//                        return new Tuple2<String, String>(dateHour, aggrInfo);
//                    }
//                });
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Iterator<Tuple2<String, String>> iterator) throws Exception {
                        List<Tuple2<String, String>> list = new ArrayList<>();

                        while (iterator.hasNext()) {

                            Tuple2<String, String> tuple = iterator.next();

                            String aggrInfo = tuple._2;
                            String startTime = StringUtils.getFieldFromConcatString(aggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_START_TIME);
                            String dateHour = DateUtils.getDateHour(startTime);

                            list.add(new Tuple2<String, String>(dateHour, aggrInfo));

                        }

                        return list;
                    }
                });
        // 得到每小时的session数量
        Map<String, Object> countByKey = time2aggrInfoRDD.countByKey();

        // 第二步，使用按时间比例随机抽取算法，计算出每天每小时抽取session的索引
        // 将<yyyy-MM-dd_HH,count> 格式的map转换成<yyyy-MM-dd,<HH,count>>格式
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();
        for (Map.Entry<String, Object> countEntry : countByKey.entrySet()) {

            String dateHour = countEntry.getKey();
            String date = dateHour.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR)[0];
            String hour = dateHour.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR)[1];

            long count = Long.parseLong(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        // 总共要抽取100 个，计算每天需要几个
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        Random random = new Random();

        /***
         * session 随机抽取功能
         *
         * 用了一个比较大的变量，随机抽取索引map
         * 之前是直接在算子里面使用了这个map，那么根据原理可知，每个task都会拷贝一份map副本，消耗内存和网络传输性能
         */
        // <date,<hour,(3,5,10)>>
        final Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的总session
            Long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            // 得到这一天，各个小时抽取的数量
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每小时
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                // 当前小时要抽取的数量 = 每小时的session数量占据当天session数量的比例 * 每天要抽取的数量
                int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                // 先获取当前小时存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (CollectionUtils.isEmpty(extractIndexList)) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量相应的随机数个数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);

                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }

                    extractIndexList.add(extractIndex);
                }
            }
        }

        // fastutil的使用，比如：List<Integer>的list --> IntList
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap = new HashMap<>();
        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();

            Map<String, IntList> fastutilHourExtractMap = new HashMap<>();
            for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();
                List<Integer> extractList = hourExtractEntry.getValue();

                IntList fastutilExtractList = new IntArrayList();
                for (int i = 0; i < extractList.size(); i++) {
                    fastutilExtractList.add(extractList.get(i));
                }

                fastutilHourExtractMap.put(hour, fastutilExtractList);
            }

            fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
        }


        // 广播 dateHourExtractMap 变量，注意理解背后的原理
        final Broadcast<Map<String, Map<String, IntList>>>
                dateHourExtractMapBroadcast = sc.broadcast(fastutilDateHourExtractMap);



        // 第三步，遍历每天每小时的，然后根据随机索引进行抽取
        // 执行 groupByKey 算子，得到<dataHour,(session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2aggrInfoRDD.groupByKey();

        /**
         * 利用flat Map算子，遍历所有的<dataHour,(session aggrInfo)> 格式数据
         * 得到每天每小时的session
         * 如果发现某个session刚好在指定某天某小时随机抽取的索引上，
         * 则抽取该session 并 写入 MySQL
         *
         * 1，将sessionId 抽取出来，形成一个新的Java RDD<String>
         * 2，用这些sessionId 去join他们的访问行为的明细数据
         *
         */
        JavaPairRDD<String, String> extractSessionIdsRDD = time2sessionsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                        List<Tuple2<String, String>> extractSessionIds = new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple2._1;
                        String date = dateHour.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR)[0];
                        String hour = dateHour.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR)[1];

                        Iterator<String> iterator = tuple2._2.iterator();

                        /**
                         * 使用广播变量
                         * 直接调用广播变量的value()方法
                         */
                        Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.value();

                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

                        int index = 0;
                        while (iterator.hasNext()) {

                            String sessionAggrInfo = iterator.next();

                            if (extractIndexList.contains(index)) {

                                // 读取相关参数值
                                String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_SESSION_ID);
                                String startTime = StringUtils.getFieldFromConcatString(sessionAggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_START_TIME);
                                String clickCategoryId = StringUtils.getFieldFromConcatString(sessionAggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_CLICK_CATEGORY);
                                String searchKeyward = StringUtils.getFieldFromConcatString(sessionAggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_SEARCH_KEYWORD);

                                // 将数据写入 MySQL
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskId(taskId);
                                sessionRandomExtract.setClickCategoryIds(clickCategoryId);
                                sessionRandomExtract.setSearchKeywords(searchKeyward);
                                sessionRandomExtract.setSessionId(sessionId);
                                sessionRandomExtract.setStartTime(startTime);
                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                // 加入list返回
                                extractSessionIds.add(new Tuple2<String, String>(sessionId, sessionId));
                            }

                            index++;
                        }

                        return extractSessionIds;
                    }
                });

        /**
         * 第四步，获取抽取出来的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractDetailRDD = extractSessionIdsRDD.join(sessionId2ActionRDD);

        extractDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {

                List<SessionDetail> sessionDetails = new ArrayList<>();

                while (iterator.hasNext()) {

                    Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();

                    Row row = tuple._2._2;

                    SessionDetail sessionDetail = new SessionDetail();
                    sessionDetail.setTaskId(taskId);
                    sessionDetail.setUserId(row.getLong(1));
                    sessionDetail.setSessionId(row.getString(2));
                    sessionDetail.setPageId(row.getLong(3));
                    sessionDetail.setActionTime(row.getString(4));
                    sessionDetail.setSearchKeyword(row.getString(5));
                    sessionDetail.setClickCategoryId(row.getLong(6));
                    sessionDetail.setClickProductId(row.getLong(7));
                    sessionDetail.setOrderCategoryIds(row.getString(8));
                    sessionDetail.setOrderProductIds(row.getString(9));
                    sessionDetail.setPayCategoryIds(row.getString(10));
                    sessionDetail.setPayProductIds(row.getString(11));

                    sessionDetails.add(sessionDetail);
                }

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insertBatch(sessionDetails);
            }
        });
    }

    /**
     * 计算出各个范围的session占比，并写入Mysql
     *
     * @param value
     * @param taskId
     */
    private static void calculateAndPersistAggrStat(String value, Integer taskId) {

        int session_count = Integer.parseInt(StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.TIME_PERIOD_30m));


        long step_length_1_3 = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.parseLong(
                StringUtils.getFieldFromConcatString(value, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.STEP_PERIOD_60));


        double visit_length_1s_3s_radio =
                NumberUtils.divOp(visit_length_1s_3s, session_count, 2).doubleValue();
        double visit_length_4s_6s_radio =
                NumberUtils.divOp(visit_length_4s_6s, session_count, 2).doubleValue();
        double visit_length_7s_9s_radio =
                NumberUtils.divOp(visit_length_7s_9s, session_count, 2).doubleValue();
        double visit_length_10s_30s_radio =
                NumberUtils.divOp(visit_length_10s_30s, session_count, 2).doubleValue();
        double visit_length_30s_60s_radio =
                NumberUtils.divOp(visit_length_30s_60s, session_count, 2).doubleValue();
        double visit_length_1m_3m_radio =
                NumberUtils.divOp(visit_length_1m_3m, session_count, 2).doubleValue();
        double visit_length_3m_10m_radio =
                NumberUtils.divOp(visit_length_3m_10m, session_count, 2).doubleValue();
        double visit_length_10m_30m_radio =
                NumberUtils.divOp(visit_length_10m_30m, session_count, 2).doubleValue();
        double visit_length_30m_radio =
                NumberUtils.divOp(visit_length_30m, session_count, 2).doubleValue();


        double step_length_1_3_radio =
                NumberUtils.divOp(step_length_1_3, session_count, 2).doubleValue();
        double step_length_4_6_radio =
                NumberUtils.divOp(step_length_4_6, session_count, 2).doubleValue();
        double step_length_7_9_radio =
                NumberUtils.divOp(step_length_7_9, session_count, 2).doubleValue();
        double step_length_10_30_radio =
                NumberUtils.divOp(step_length_10_30, session_count, 2).doubleValue();
        double step_length_30_60_radio =
                NumberUtils.divOp(step_length_30_60, session_count, 2).doubleValue();
        double step_length_60_radio =
                NumberUtils.divOp(step_length_60, session_count, 2).doubleValue();

        // 写入MySQL数据库
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskId(taskId);
        sessionAggrStat.setSessionCount(session_count);
        sessionAggrStat.setStep_length_1_3(step_length_1_3_radio);
        sessionAggrStat.setStep_length_4_6(step_length_4_6_radio);
        sessionAggrStat.setStep_length_7_9(step_length_7_9_radio);
        sessionAggrStat.setStep_length_10_30(step_length_10_30_radio);
        sessionAggrStat.setStep_length_30_60(step_length_30_60_radio);
        sessionAggrStat.setStep_length_60(step_length_60_radio);
        sessionAggrStat.setVisit_length_1s_3s(visit_length_1s_3s_radio);
        sessionAggrStat.setVisit_length_4s_6s(visit_length_4s_6s_radio);
        sessionAggrStat.setVisit_length_7s_9s(visit_length_7s_9s_radio);
        sessionAggrStat.setVisit_length_10s_30s(visit_length_10s_30s_radio);
        sessionAggrStat.setVisit_length_30s_60s(visit_length_30s_60s_radio);
        sessionAggrStat.setVisit_length_1m_3m(visit_length_1m_3m_radio);
        sessionAggrStat.setVisit_length_3m_10m(visit_length_3m_10m_radio);
        sessionAggrStat.setVisit_length_10m_30m(visit_length_10m_30m_radio);
        sessionAggrStat.setVisit_length_30m(visit_length_30m_radio);

        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.add(sessionAggrStat);

    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext SQLContext
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(
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

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param sqlContext
     * @param sessionid2ActionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(
            SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2ActionRDD) {
        // 现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        // 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =
                sessionid2ActionRDD.groupByKey();

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(

                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
                            throws Exception {

                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer();
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer();

                        Long userId = null;

                        // session的起始和结束时间
                        Date startTime = null;
                        Date endTime = null;
                        // session 步长
                        int stepLength = 0;


                        // 遍历session所有的访问行为
                        while (iterator.hasNext()) {

                            // 提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if (userId == null) {
                                userId = row.getLong(1);
                            }

                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getLong(6);

                            // 实际上这里要对数据说明一下
                            // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                            // 其实，只有搜索行为，是有searchKeyword字段的
                            // 只有点击品类的行为，是有clickCategoryId字段的
                            // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                            // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                            // 首先要满足：不能是null值
                            // 其次，之前的字符串中还没有搜索词或者点击品类id

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }
                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString().contains(
                                        String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 计算session开始和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));
                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }
                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            // 计算session访问步长
                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 计算session访问时长（秒）
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        // 我们返回的数据格式，即使<sessionId,partAggrInfo>
                        // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
                        // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                        // 就应该是userid，才能够跟<userId,Row>格式的用户信息进行聚合
                        // 如果我们这里直接返回<sessionId,partAggrInfo>，还得再做一次mapToPair算子
                        // 将RDD映射成<userId,partAggrInfo>的格式，那么就多此一举

                        // 所以，我们这里其实可以直接，返回的数据格式，就是<userId,partAggrInfo>
                        // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                        // 然后再直接将返回的Tuple的key设置成sessionid
                        // 最后的数据格式，还是<sessionId,fullAggrInfo>

                        // 聚合数据，用什么样的格式进行拼接？
                        // 我们这里统一定义，使用key=value|key=value
                        String partAggrInfo = Constants.FIELD_SESSION_ID + Constants.SYMBAL_EQUALS_SIGN + sessionId + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_SEARCH_KEYWORD + Constants.SYMBAL_EQUALS_SIGN + searchKeywords + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_CLICK_CATEGORY + Constants.SYMBAL_EQUALS_SIGN + clickCategoryIds + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_VISIT_LENGTH + Constants.SYMBAL_EQUALS_SIGN + visitLength + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_STEP_LENGTH + Constants.SYMBAL_EQUALS_SIGN + stepLength + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_START_TIME + Constants.SYMBAL_EQUALS_SIGN + DateUtils.formatTime(startTime);

                        return new Tuple2<Long, String>(userId, partAggrInfo);
                    }

                });

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(

                new PairFunction<Row, Long, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }

                });

        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
                userid2PartAggrInfoRDD.join(userid2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<Long, Tuple2<String, Row>> tuple)
                            throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString(
                                partAggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_AGE + Constants.SYMBAL_EQUALS_SIGN + age + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_PROFESSIONAL + Constants.SYMBAL_EQUALS_SIGN + professional + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_CITY + Constants.SYMBAL_EQUALS_SIGN + city + Constants.SYMBAL_VERTICAL_BAR
                                + Constants.FIELD_SEX + Constants.SYMBAL_EQUALS_SIGN + sex;

                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }

                });

        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 过滤session数据
     *
     * @param sessionid2AggrInfoRDD
     * @param sessionAggrStatAccumulator
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + Constants.SYMBAL_EQUALS_SIGN + startAge + Constants.SYMBAL_VERTICAL_BAR : "")
                + (endAge != null ? Constants.PARAM_END_AGE + Constants.SYMBAL_EQUALS_SIGN + endAge + Constants.SYMBAL_VERTICAL_BAR : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + Constants.SYMBAL_EQUALS_SIGN + professionals + Constants.SYMBAL_VERTICAL_BAR : "")
                + (cities != null ? Constants.PARAM_CITIES + Constants.SYMBAL_EQUALS_SIGN + cities + Constants.SYMBAL_VERTICAL_BAR : "")
                + (sex != null ? Constants.PARAM_SEX + Constants.SYMBAL_EQUALS_SIGN + sex + Constants.SYMBAL_VERTICAL_BAR : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + Constants.SYMBAL_EQUALS_SIGN + keywords + Constants.SYMBAL_VERTICAL_BAR : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + Constants.SYMBAL_EQUALS_SIGN + categoryIds : "");

        if (_parameter.endsWith(Constants.SPLIT_SYMBAL_VERTICAL_BAR)) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String, String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先，从tuple中，获取聚合数据
                        String aggrInfo = tuple._2;

                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // 我们的session可能搜索了 火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORD,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        // 用户需要的数据
                        // 总的session数量
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                        // 计算出session的访问时长和访问步长范围，并进行相应的累加
                        long visitLength = Long.valueOf(StringUtils.
                                getFieldFromConcatString(aggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.
                                getFieldFromConcatString(aggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_STEP_LENGTH));

                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }


                });

        return filteredSessionid2AggrInfoRDD;
    }


    /**
     * 过滤session数据
     *
     * @param sessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSession(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + Constants.SYMBAL_EQUALS_SIGN + startAge + Constants.SYMBAL_VERTICAL_BAR : "")
                + (endAge != null ? Constants.PARAM_END_AGE + Constants.SYMBAL_EQUALS_SIGN + endAge + Constants.SYMBAL_VERTICAL_BAR : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + Constants.SYMBAL_EQUALS_SIGN + professionals + Constants.SYMBAL_VERTICAL_BAR : "")
                + (cities != null ? Constants.PARAM_CITIES + Constants.SYMBAL_EQUALS_SIGN + cities + Constants.SYMBAL_VERTICAL_BAR : "")
                + (sex != null ? Constants.PARAM_SEX + Constants.SYMBAL_EQUALS_SIGN + sex + Constants.SYMBAL_VERTICAL_BAR : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + Constants.SYMBAL_EQUALS_SIGN + keywords + Constants.SYMBAL_VERTICAL_BAR : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + Constants.SYMBAL_EQUALS_SIGN + categoryIds : "");

        if (_parameter.endsWith(Constants.SPLIT_SYMBAL_VERTICAL_BAR)) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String, String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先，从tuple中，获取聚合数据
                        String aggrInfo = tuple._2;

                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // 我们的session可能搜索了 火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORD,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        return true;
                    }

                });

        return filteredSessionid2AggrInfoRDD;
    }


}
