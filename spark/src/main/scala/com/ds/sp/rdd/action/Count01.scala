package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/28 16 50
 * @Author: devil
 * @version: v1.0.0
 * */
object Count01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local").setAppName("count demo")
    val sc = new SparkContext(conf)
    val rddCount: Long = sc.makeRDD(List(1, 2, 3, 4), 2).count()
    println(rddCount)
  }

}
