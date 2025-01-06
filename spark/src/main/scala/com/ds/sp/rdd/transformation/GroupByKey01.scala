package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 10 54
 * @Author: devil
 * @version: v1.0.0
 * */
object GroupByKey01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setAppName("groupBy key").setMaster("local")
    val sc = new SparkContext(conf)

    sc.makeRDD(List((1,"A"),(2,"B"),(3,"C "),(1,"D"),(2,"C")),2)
      .groupByKey()
      .collect()
      .foreach(println)


    sc.makeRDD(List((1, "A"), (2, "B"), (3, "C "), (1, "D"), (2, "C")), 2)
      .groupBy(_._2)
      .collect()
      .foreach(println)
  }

}
