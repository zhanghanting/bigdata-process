package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: 函数详解：
对 RDD 的分区进行重新分区，shuffle 默认值为 false，当 shuffle =false 时，不能增加分区 数目 ，但不会报错 ，
  只是分区个数还是原来的 ，但是分区数少于之前的分区是没有问题的 ，即多 分区合并 ，不会产生 shuffle。
 * @Create on: 2024/12/27 21 44
 * @Author: devil
 * @version: v1.0.0
 * 开发怎么用：如果源文件有多个小文件，可以用此函数减少 read task 的数量，减少数据 shuffle，
 * 比如有些时候，在很多 partition 的时候，我们想减少点 partition 的数量，不然写到 HDFS 上的  文件数量也会很多很多。
 * */
object Coalesce01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("coalesce")
    val sc = new SparkContext(conf)
    val coalesceRDD = sc.makeRDD(Array(1, 2, 3, 4, 5), 2)
      .coalesce(3)

    println("RDD分区个数：" + coalesceRDD.partitions.size)
    println("RDD依赖关系：" + coalesceRDD.toDebugString)
  }

}
