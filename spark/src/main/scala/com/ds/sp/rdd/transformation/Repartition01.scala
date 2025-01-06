package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/27 22 05
 * @Author: devil
 * @version: v1.0.0
 * repartition底层调用了coalesce，shuffle参数为true,所以会产生shuffle
 * */
object Repartition01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("coalesce")
    val sc = new SparkContext(conf)
    val repartitionRDD = sc.makeRDD(Array(1, 2, 3, 4, 5), 2)
      .repartition(3)

    println("RDD分区个数：" + repartitionRDD.partitions.size)
    println("RDD依赖关系：" + repartitionRDD.toDebugString)
  }

}
