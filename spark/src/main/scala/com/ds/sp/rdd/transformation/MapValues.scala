package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 10 33
 * @Author: devil
 * @version: v1.0.0
 * */
object MapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local").setAppName("mapValues")
    val sc = new SparkContext(conf)

    sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5), ("f", 6)))
      .mapValues(_ * 3).collect()
      .foreach(println)


    sc.makeRDD(List(("a", "hello java"), ("b", "hello scala"), ("c", "hello hadoop")))
      .flatMapValues(_.split(" "))
      .collect()
      .foreach(println)
  }

}
