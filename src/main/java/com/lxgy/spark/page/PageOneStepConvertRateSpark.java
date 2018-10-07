package com.lxgy.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.lxgy.spark.constant.Constants;
import com.lxgy.spark.dao.impl.DAOFactory;
import com.lxgy.spark.domain.PageSplitConvertRate;
import com.lxgy.spark.domain.Task;
import com.lxgy.spark.utils.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * @author Gryant
 */
public class PageOneStepConvertRateSpark {

    public static void main(String[] args) {

        args = new String[]{"2"};

        // 1，构造Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        // 2，生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 3，查询任务，获取任务的参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = DAOFactory.getTaskDAO().findById(taskId);
        if (task == null || StringUtils.isEmpty(task.getTaskParam())) {
            System.out.println(DateUtils.getCurDateTime() + "cannot find task by task_id:" + taskId);
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());


        // 4，查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

        // 获取session与对应用户访问行为的数据
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);

        // 拿到每个session对应的访问行为数据，才能够去生成切片
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessionId2ActionRDD.groupByKey();
        // --> persist(StorageLevel.MEMORY_ONLY)
        sessionId2ActionsRDD = sessionId2ActionsRDD.cache();

        // 页面切片生成与匹配算法
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc, sessionId2ActionsRDD, taskParam);
        Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();

        // 使用者指定的页面流是3，2，5，8，6
        // 目前的pageSplitPvMap：3->2,2->5...
        Long startPagePv = getStartPagePv(taskParam, sessionId2ActionsRDD);

        // 计算页面切片转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv);

        // 数据写库
        StringBuffer convertRateStr = new StringBuffer();
        for (Map.Entry<String, Double> entry : convertRateMap.entrySet()) {
            convertRateStr.append(entry.getKey())
                    .append(Constants.SYMBAL_EQUALS_SIGN)
                    .append(entry.getValue()).append(Constants.SYMBAL_VERTICAL_BAR);
        }

        PageSplitConvertRate convertRate = new PageSplitConvertRate(taskId, convertRateStr.deleteCharAt(convertRateStr.length() - 1).toString());
        DAOFactory.getPageSplitConvertRateDAO().insert(convertRate);

    }

    /**
     * 计算页面切片转化率
     *
     * @param taskParam
     * @param pageSplitPvMap
     * @param startPagePv
     * @return
     */
    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPvMap, Long startPagePv) {

        Map<String, Double> convertRateMap = new HashMap<>();

        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(Constants.SPLIT_SYMBAL_COMMA);

        Long lastPageSplitPv = null;
        for (int i = 1; i < targetPages.length; i++) {

            String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
            Long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));

            Double convertRate = 0.0;

            if (i == 1) {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) startPagePv, 2);
            } else {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) lastPageSplitPv, 2);
            }

            lastPageSplitPv = targetPageSplitPv;

            convertRateMap.put(targetPageSplit, convertRate);
        }

        return convertRateMap;
    }

    /**
     * 获取其实页面pv
     * @param taskParam
     * @param sessionId2ActionsRDD
     * @return
     */
    private static Long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Long startPageId = Long.parseLong(targetPageFlow.split(Constants.SPLIT_SYMBAL_COMMA)[0]);

        JavaRDD<Long> startPageRDD = sessionId2ActionsRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            @Override
            public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                List<Long> list = new ArrayList<>();

                Iterator<Row> iterator = tuple._2.iterator();

                while (iterator.hasNext()) {
                    Long pageId = iterator.next().getLong(3);
                    if (startPageId.equals(pageId)) {
                        list.add(pageId);
                    }
                }
                return list;
            }
        });

        return startPageRDD.count();
    }

    /**
     * 页面切片生成与匹配算法
     *
     * @param sc
     * @param sessionId2ActionsRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaSparkContext sc, JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD, JSONObject taskParam) {

        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

        return sessionId2ActionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                List<Tuple2<String, Integer>> list = new ArrayList<>();

                // 获取当前session的访问行为的迭代器
                Iterator<Row> iterator = tuple._2.iterator();

                // 获取使用者指定的页面流，比如：1，2，3，4，5 1-->2 转化率为？
                String[] targetPages = targetPageFlowBroadcast.value().split(Constants.SPLIT_SYMBAL_COMMA);

                // 这里我们获取的session访问行为数据默认情况下是乱序的，所以需要对数据按时间排序
                List<Row> rows = new ArrayList<>();
                while (iterator.hasNext()) {
                    rows.add(iterator.next());
                }

                // 使用默认升序排序
                Collections.sort(rows, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String actionTimeStr1 = o1.getString(4);
                        String actionTimeStr2 = o2.getString(4);

                        Date actionTime1 = DateUtils.parseTime(actionTimeStr1);
                        Date actionTime2 = DateUtils.parseTime(actionTimeStr2);

                        return (int) (actionTime1.getTime() - actionTime2.getTime());
                    }
                });

                // 切片匹配算法
                Long lastPageId = null;
                for (Row row : rows) {
                    Long pageId = row.getLong(3);
                    if (lastPageId == null) {
                        lastPageId = pageId;
                        continue;
                    }

                    // 生成一个页面切片
                    // 3，5，2，1，4，9
                    // 切片 3_5
                    String pageSplit = lastPageId + Constants.SPLIT_SYMBAL_UNDERLINE_BAR + pageId;

                    // 判断当前切片是否在用户指定的页面流中
                    for (int i = 1; i < targetPages.length; i++) {

                        String targetPageSplit = targetPages[i - 1] + Constants.SPLIT_SYMBAL_UNDERLINE_BAR + targetPages[i];

                        // 匹配
                        if (pageSplit.equals(targetPageSplit)) {
                            list.add(new Tuple2<String, Integer>(pageSplit, 1));
                            break;
                        }
                    }

                    // 如果没有匹配上，将其设置为上一个
                    lastPageId = pageId;
                }

                return list;
            }
        });
    }

    /**
     * 获取<session,用户访问行为>格式的数据
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
    }


}
