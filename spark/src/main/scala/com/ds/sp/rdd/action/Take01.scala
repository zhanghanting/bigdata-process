package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/28 16 51
 * @Author: devil
 * @version: v1.0.0
 * */
object Take01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local").setAppName("take demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(List(1, 3, 4, 5, 6), 3).take(4)
      .foreach(println)

    println("====================")
    sc.makeRDD(List(1, 3, 4, 5, 6), 3).takeSample(false, 3)
      .foreach(println)
  }

}
