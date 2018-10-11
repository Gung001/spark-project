package com.lxgy.spark.product;

import com.lxgy.spark.constant.Constants;
import org.apache.spark.sql.api.java.UDF1;

import java.util.Random;

/**
 * @author Gryant
 */
public class RemoveRandomPrefixUDF implements UDF1<String,String> {
    @Override
    public String call(String val) throws Exception {
        return val.split(Constants.SPLIT_SYMBAL_UNDERLINE_BAR)[1];
    }
}
