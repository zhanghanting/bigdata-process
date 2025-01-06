package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 10 10
 * @Author: devil
 * @version: v1.0.0
 * */
object Zip01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setAppName("zip").setMaster("local")
    val sc = new SparkContext(conf)

    val dataRDD1 = sc.makeRDD(List(1, 2, 3, 4))
    val dataRDD2 = sc.makeRDD(List(3, 4, 5, 6))
    dataRDD1.zip(dataRDD2)
      .collect()
      .foreach(println)
  }

}
