package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/28 16 30
 * @Author: devil
 * @version: v1.0.0
 * */
object Collect01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local[*]").setAppName("collect demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(List(1, 2, 3, 4), 2).map(_ * 2)
      .collect()//返回的是一个array
      .foreach(println)
  }

}
