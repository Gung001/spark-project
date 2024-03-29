package com.lxgy.spark.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.alibaba.fastjson.JSON;
import com.lxgy.spark.dao.impl.SessionAggrStatDAOImpl;
import com.lxgy.spark.utils.DateUtils;
import com.lxgy.spark.utils.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 模拟数据程序
 * @author Administrator
 *
 */
public class MockData {

	private final static Logger logger = LoggerFactory.getLogger(MockData.class);


	/**
	 * 模拟数据
	 * @param sc
	 * @param sqlContext
	 */
	public static void mock(JavaSparkContext sc,
			SQLContext sqlContext) {
		List<Row> rows = new ArrayList<Row>();
		
		String[] searchKeywords = new String[] {"足球", "篮球", "乒乓球", "羽毛球",
				"排球", "冰球", "曲棍球", "棒球", "手球", "铅球"};
		String date = DateUtils.getTodayDate();
		logger.debug("即将生成的数据日期是：" + date);
		String[] actions = new String[]{"search", "click", "order", "pay"};
		Random random = new Random();
		
		for(int i = 0; i < 100; i++) {

			// 随机生成userId
			long userId = random.nextInt(100);
			
			for(int j = 0; j < 10; j++) {

				// 随机生成sessionId
				String sessionId = UUID.randomUUID().toString().replace("-", "");
				String baseActionTime = date + " " + random.nextInt(23);
				  
				for(int k = 0; k < random.nextInt(100); k++) {

					long pageId = random.nextInt(10);
					String actionTime = baseActionTime + ":" +
							StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" +
							StringUtils.fulfuill(String.valueOf(random.nextInt(59)));

					String searchKeyword = null;
					Long clickCategoryId = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;
					
					String action = actions[random.nextInt(4)];
					if("search".equals(action)) {
						searchKeyword = searchKeywords[random.nextInt(10)];   
					} else if("click".equals(action)) {
						clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));    
						clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));  
					} else if("order".equals(action)) {
						orderCategoryIds = String.valueOf(random.nextInt(100));  
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if("pay".equals(action)) {
						payCategoryIds = String.valueOf(random.nextInt(100));  
						payProductIds = String.valueOf(random.nextInt(100));
					}
					
					Row row = RowFactory.create(date, userId, sessionId,
							pageId, actionTime, searchKeyword,
							clickCategoryId, clickProductId,
							orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds);
					rows.add(row);
				}
			}
		}
		
		JavaRDD<Row> rowsRDD = sc.parallelize(rows);
		
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)));
		
		DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);
		
		df.registerTempTable("user_visit_action");  
		for(Row _row : df.take(1)) {
			System.out.println(_row);  
		}
		
		/**
		 * ==================================================================
		 */
		
		rows.clear();
		String[] sexes = new String[]{"male", "female"};
		for(int i = 0; i < 100; i ++) {
			long userId = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = random.nextInt(60);
			String professional = "professional" + random.nextInt(100);
			String city = "city" + random.nextInt(100);
			String sex = sexes[random.nextInt(2)];
			
			Row row = RowFactory.create(userId, username, name, age,
					professional, city, sex);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("professional", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true)));
		
		DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
		for(Row _row : df2.take(1)) {
			logger.debug(JSON.toJSONString(_row));
		}
		
		df2.registerTempTable("user_info");  
	}
	
}
