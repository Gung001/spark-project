package com.lxgy.spark.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxgy.spark.conf.ConfigurationManager;
import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.dao.ISessionAggrStatDAO;
import com.lxgy.spark.dao.ISessionDetailDAO;
import com.lxgy.spark.dao.ISessionRandomExtractDAO;
import com.lxgy.spark.dao.ITaskDAO;
import com.lxgy.spark.dao.impl.DAOFactory;
import com.lxgy.spark.domain.SessionAggrStat;
import com.lxgy.spark.domain.SessionDetail;
import com.lxgy.spark.domain.SessionRandomExtract;
import com.lxgy.spark.domain.Task;
import com.lxgy.spark.mock.MockData;
import com.lxgy.spark.utils.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
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
 * @author Administrator
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        args = new String[]{"1"};

        // 构建Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
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

        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        // 获取sessionId映射的明细
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);

        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);
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

        randomExtractSession(task.getTaskId(), filteredSessionid2AggrInfoRDD, sessionId2ActionRDD);

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


        // 关闭Spark上下文
        sc.close();
    }

    /**
     * 获取sessionId到访问行为数据映射的RDD
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
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
    private static void randomExtractSession(final Integer taskId, JavaPairRDD<String, String> sessionid2AggrInfoRDD, JavaPairRDD<String, Row> sessionId2ActionRDD) {

        // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>RDD
        JavaPairRDD<String, String> time2aggrInfoRDD = sessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {

                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {

                        String aggrInfo = tuple2._2;
                        String startTime = StringUtils.getFieldFromConcatString(aggrInfo, Constants.SPLIT_SYMBAL_VERTICAL_BAR, Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);

                        return new Tuple2<String, String>(dateHour, aggrInfo);
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

        // 第三步，遍历每天每小时的session，然后根据随机索引进行抽取
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

        extractDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {

                Row row = tuple2._2._2;

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

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);

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

        return actionDF.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param actionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(
            SQLContext sqlContext, JavaRDD<Row> actionRDD) {
        // 现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        // 我们现在需要将这个Row映射成<sessionid,Row>的格式
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(

                /**
                 * PairFunction
                 * 第一个参数，相当于是函数的输入
                 * 第二个参数和第三个参数，相当于是函数的输出（Tuple），分别是Tuple第一个和第二个值
                 */
                new PairFunction<Row, String, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(2), row);
                    }

                });

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

                        String sessionid = com.lxgy.spark.utils.StringUtils.getFieldFromConcatString(
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
