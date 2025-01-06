package com.ds.sp.rdd.checkpoint

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.checkpoint
 * @Description: TODO
 * @Create on: 2025/1/2 20 49
 * @Author: devil
 * @version: v1.0.0
 *
 * cache目的是为了提高效率，重复使用的算子算一遍重复利用。数据一般缓存在内存或者磁盘中，运行完成后cache的结果会立即删除
 *
 * Checkpoint目的是为了容错，防止发生错误时从头开始计算，所以Checkpoint会切断血缘，一旦发生异常，在Checkpoint位置重新开始计算即可，一般Checkpoint的数据都持久化在hdfs上
 *
 * set spark.cleaner.referenceTracking.cleanCheckpoints = true
 * */
object Checkpoint01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("checkpoint demo")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints","true")
      //.set("spark.local.dir", "spark/src/main/resources/data")

    val sc = new SparkContext(conf)


    sc.setCheckpointDir("spark/src/main/resources/data/")


    val filterRDD= sc.textFile("spark/src/main/resources/data/gp.txt")
      .filter(_.startsWith("A"))

    filterRDD.checkpoint()
    println(filterRDD.count())
    println(filterRDD.count())


  }

}
