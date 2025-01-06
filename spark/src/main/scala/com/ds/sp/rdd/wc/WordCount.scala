package com.ds.sp.rdd.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.wc
 * @Create on: 2024/12/30 20 43
 * @Author: devil
 * @version: v1.0.0
 * */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)

    val path = this.getClass.getResource("/data/wc.txt").getPath

    sc.textFile(path)
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(println)
  }

}
