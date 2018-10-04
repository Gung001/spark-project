package com.lxgy.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Gryant
  */
object SortKeyTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SortKeyTest")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val testArr = Array(
      Tuple2(new SortKey(20, 19, 13), "1"),
      Tuple2(new SortKey(20, 18, 12), "2"),
      Tuple2(new SortKey(30, 11, 13), "3"))

    val testRDD = sc.parallelize(testArr, 1)

    val sortedRDD = testRDD.sortByKey(false)

    for (tuple <- sortedRDD.collect()) println(tuple._2)
  }
}
