package com.ds.sp.rdd.sharedvariable

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.sharedvariable
 * @Create on: 2025/1/3 17 28
 * @Author: devil
 * @version: v1.0.0
 * */
object Accumulator01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("accumulator demo")

    val sc = new SparkContext(conf)

   /* val sumAcc = sc.longAccumulator("sumAcc")

    sc.makeRDD(List(1,2,3,4,5))
      .foreach(num => sumAcc.add(num))

    println(sumAcc)*/

    val mapAcc = sc.collectionAccumulator[String]("map")

    sc.makeRDD(List("a","b","c","d"))
      .foreach(word => mapAcc.add(word))
    println(mapAcc)
  }
}
