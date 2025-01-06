package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/28 21 46
 * @Author: devil
 * @version: v1.0.0
 * */
object Reduce01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("reduce demo").setMaster("local")
    val sc = new SparkContext(conf)
    val reduce: Int = sc.makeRDD(Array(1, 2, 4, 5, 3)).reduce(_ + _)
    println(reduce)
  }

}
