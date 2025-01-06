package com.ds.sp.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/28 21 51
 * @Author: devil
 * @version: v1.0.0
 * 备注：和aggregateByKey不同的是，aggregate 的初始值既是提供给分区内做计算又提供给分区间的计算
 * */
object Aggregate01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aggregate demo").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 将该  RDD 所有元素相加得到结果,对于两个函数相同的情况下，aggregate 可以简化为fold
    println(rdd.aggregate(0)(_ + _, _ + _))
    println(rdd.fold(0)(_ + _))
  }

}
