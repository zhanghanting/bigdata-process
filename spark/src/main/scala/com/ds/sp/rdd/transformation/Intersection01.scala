package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 09 56
 * @Author: devil
 * @version: v1.0.0
 * 求交集
 * */
object Intersection01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("intersection")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5))
    val rdd2 = sc.makeRDD(List(6,7, 3, 4, 5))

    rdd1.intersection(rdd2)
      .collect()
      .foreach(println)
  }

}
