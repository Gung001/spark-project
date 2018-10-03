package com.lxgy.spark.utils;

import com.lxgy.spark.constant.Constants;

/**
 * 字符串工具类
 *
 * @author Administrator
 */
public class StringUtils {

    /**
     * 判断字符串是否为空
     *
     * @param str 字符串
     * @return 是否为空
     */
    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    /**
     * 判断字符串是否不为空
     *
     * @param str 字符串
     * @return 是否不为空
     */
    public static boolean isNotEmpty(String str) {
        return str != null && !"".equals(str);
    }

    /**
     * 截断字符串两侧的逗号
     *
     * @param str 字符串
     * @return 字符串
     */
    public static String trimComma(String str) {
        if (str.startsWith(",")) {
            str = str.substring(1);
        }
        if (str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     *
     * @param str
     * @return
     */
    public static String fulfuill(String str) {
        if (str.length() == 2) {
            return str;
        } else {
            return "0" + str;
        }
    }

    /**
     * 从拼接的字符串中提取字段
     *
     * @param str       字符串
     * @param delimiter 分隔符
     * @param field     字段
     * @return 字段值
     */
    public static String getFieldFromConcatString(String str,
                                                  String delimiter, String field) {
        String[] fields = str.split(delimiter);
        for (String concatField : fields) {
            if (concatField.split(Constants.SYMBAL_EQUALS_SIGN).length == 2) {
                String fieldName = concatField.split(Constants.SYMBAL_EQUALS_SIGN)[0];
                String fieldValue = concatField.split(Constants.SYMBAL_EQUALS_SIGN)[1];
                if (fieldName.equals(field)) {
                    return fieldValue;
                }
            }
        }
        return null;
    }

    /**
     * 从拼接的字符串中给字段设置值
     *
     * @param str           字符串
     * @param delimiter     分隔符
     * @param field         字段名
     * @param newFieldValue 新的field值
     * @return 字段值
     */
    public static String setFieldInConcatString(String str,
                                                String delimiter, String field, String newFieldValue) {
        String[] fields = str.split(delimiter);

        // 找到key=value 完成赋值
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].split(Constants.SYMBAL_EQUALS_SIGN)[0];
            if (fieldName.equals(field)) {
                String concatField = fieldName + Constants.SYMBAL_EQUALS_SIGN + newFieldValue;
                fields[i] = concatField;
                break;
            }
        }

        // 拼接成一个字符串
        StringBuffer buffer = new StringBuffer("");
        for (int i = 0; i < fields.length; i++) {
            buffer.append(fields[i]);
            if (i < fields.length - 1) {
                buffer.append(Constants.SYMBAL_VERTICAL_BAR);
            }
        }

        return buffer.toString();
    }

}
