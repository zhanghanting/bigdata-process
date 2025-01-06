package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 10 07
 * @Author: devil
 * @version: v1.0.0
 * */
object Cartesian01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setAppName("cartesian").setMaster("local")
    val sc = new SparkContext(conf)

    val x = sc.parallelize(List(1, 2, 3, 4, 5))
    val y = sc.parallelize(List(6, 7, 8, 9, 10))
    x.cartesian(y)
      .collect
      .foreach(println)
  }
}
