package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 09 59
 * @Author: devil
 * @version: v1.0.0
 * */
object Union01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("union")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5))
    val rdd2 = sc.makeRDD(List(6, 7, 3, 4, 5))
//类似sql的union all,想去重再使用distinct
    rdd1.union(rdd2)
      .distinct()
      .collect()
      .foreach(println)
  }

}
