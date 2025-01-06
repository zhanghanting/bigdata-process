package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 10 04
 * @Author: devil
 * @version: v1.0.0
 * */
object Subtract01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("subtract")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5))
    val rdd2 = sc.makeRDD(List(6, 7, 3, 4, 5))

    rdd1.subtract(rdd2)//1,2
      .collect()
      .foreach(println)
  }

}
