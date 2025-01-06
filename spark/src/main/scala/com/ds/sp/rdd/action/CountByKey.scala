package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/30 10 07
 * @Author: devil
 * @version: v1.0.0
 * */
object CountByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("countByKey Demo")
    val sc = new SparkContext(conf)
    val list = List((1,"a"),(1,"b"),(2,"b"),(2,"b"),(3,"c"),(4,"c"))

    sc.makeRDD(list)
      .countByKey()//返回Map[Key,Long]统计每个key出现的次数
      .foreach(println)


    println("======================================")
    sc.makeRDD(list)
      .countByValue()//统计是的每个元组出现的次数，底层还是调用的countByKey
      .foreach(println)
  }

}
