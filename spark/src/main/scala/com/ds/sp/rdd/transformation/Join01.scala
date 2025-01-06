package com.ds.sp.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 15 42
 * @Author: devil
 * @version: v1.0.0
 * */
object Join01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setAppName("join").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c"),(5,"e")))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6),(4,6)))
    rdd.join(rdd1).collect().foreach(println)//结果为内连接
  }

}
