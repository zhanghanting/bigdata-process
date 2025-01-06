package com.ds.sp.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Create on: 2024/12/28 15 37
 * @Author: devil
 * @version: v1.0.0
 * */
object CoGroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("CoGroup")
    val sc = new SparkContext(conf)

    val dataRDD1 = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3)))
    val dataRDD2 = sc.makeRDD(List(("a", 1), ("c", 2), ("b", 3)))
    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] =
      dataRDD1.cogroup(dataRDD2)
    value.collect()
      .foreach(println)
  }

}
