package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/27 21 23
 * @Author: devil
 * @version: v1.0.0
 * */
object Sample01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("glom")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
      .sample(false,0.7)//不放回抽取，抽取70%的数据（0-1）
      .collect()
      .foreach(println)
  }

}
