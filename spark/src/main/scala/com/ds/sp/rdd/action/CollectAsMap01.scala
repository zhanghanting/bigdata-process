package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/28 16 33
 * @Author: devil
 * @version: v1.0.0
 * ollectAsMap 与 collect 类似 ，主要针对元素类型为 key-value 对的 RDD ，转换为 Scala Map 并返回 ，保存元素的 KV 结构 ，作用与 collect 不同的是 collectAsMap 函数不包含
 * */
object CollectAsMap01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setAppName("collectAsMap").setMaster("local")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = sc.parallelize(arr, 2)
    rdd.collectAsMap().foreach(println)
  }

}
