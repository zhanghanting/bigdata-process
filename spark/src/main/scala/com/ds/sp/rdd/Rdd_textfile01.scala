package com.ds.sp.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd
 * @Create on: 2024/12/27 16 17
 * @Author: devil
 * @version: v1.0.0
 * */
object Rdd_textfile01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("textfile")
    val context = new SparkContext(conf)

    //val value:RDD[String] = context.textFile("hdfs://ds-bigdata-001:8020/user/zhting241208/spark_datas/a.txt")
    val value:RDD[String] = context.textFile("E:\\大数据测试数据集\\spark\\a.txt")
    value.collect.foreach(println)

  }

}
