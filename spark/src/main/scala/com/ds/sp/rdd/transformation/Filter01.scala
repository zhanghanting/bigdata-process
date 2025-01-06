package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Create on: 2024/12/27 18 01
 * @Author: devil
 * @version: v1.0.0
 * */
object Filter01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local").setAppName("filter")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array("aa", "bb", "ab", "cc"), 1)
    .filter(_.startsWith("a"))
    .foreach(println)
  }

}
