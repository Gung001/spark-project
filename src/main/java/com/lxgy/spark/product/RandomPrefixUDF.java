package com.lxgy.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * @author Gryant
 */
public class RandomPrefixUDF implements UDF2<String,Integer,String> {
    @Override
    public String call(String val, Integer num) throws Exception {
        Random random = new Random();
        int randomNum = random.nextInt(10);
        return randomNum + "_" + val;
    }
}
