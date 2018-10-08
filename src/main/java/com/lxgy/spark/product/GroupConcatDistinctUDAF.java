package com.lxgy.spark.product;

import com.lxgy.spark.constant.Constants;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.actors.threadpool.Arrays;

/**
 * 组内拼接去重函数
 *
 * @author Gryant
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

    // 指定输入数的字段与类型
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            new StructField[]{DataTypes.createStructField("cityInfo", DataTypes.StringType, true)}));

    // 指定缓冲数据的字段与类型
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
            new StructField[]{DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)}));

    // 指定返回类型
    private DataType dataType = DataTypes.StringType;

    // 指定是否是确定性的
    private Boolean deterministic = true;

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        // 缓冲中的已经拼接过的城市信息串
        String bufferCityInfo = buffer.getString(0);
        // 刚刚传递进来的某个城市的信息
        String cityInfo = input.getString(0);

        // 去重逻辑
        if (!bufferCityInfo.contains(cityInfo)) {

            if ("".equals(bufferCityInfo)) {
                bufferCityInfo += cityInfo;
            } else {
                // 比如：1:北京,2:上海
                bufferCityInfo += "," + cityInfo;
            }

            buffer.update(0, bufferCityInfo);
        }
    }

    /**
     * 合并操作：
     * update操作可能是针对一个分组内的部分数据在某个节点上发生的
     * 但是可能一个分组内的数据会分布在多个节点上处理
     * 此时就要用merge操作将各个节点上分布分布式拼接好的串合并起来
     *
     * @param buffer1
     * @param buffer2
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);

        for (String cityInfo : bufferCityInfo2.split(Constants.SPLIT_SYMBAL_COMMA)) {
            if (!bufferCityInfo1.contains(cityInfo)) {
                if ("".equals(bufferCityInfo1)) {
                    bufferCityInfo1 += cityInfo;
                } else {
                    bufferCityInfo1 += "," + cityInfo;
                }
            }
        }

        buffer1.update(0, bufferCityInfo1);
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
