package com.ds.sp.rdd.transformation

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 10 39
 * @Author: devil
 * @version: v1.0.0
 *
 * 函数详解：
*将数据（处理的数据必须要是 key-value 对）按照指定 Partitioner 重新进行分区。
*Spark 默 认的分区器是 Hash Partitioner
*什么是分区器？所谓分区器即数据是如何通过上一个分区进入下一个分区的，可以理解成数据路由。
 *
 * */
object PartitionBy01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local").setAppName("partitionBy demo")
    val sc = new SparkContext(conf)

    sc.makeRDD(List((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
      .partitionBy(new HashPartitioner(3)) //          .repartition(3)
      .mapPartitionsWithIndex{ (id,iter) =>{
        println(id + "分区： " + iter.mkString(","))
        iter
      } }
      .collect()
      .foreach(println)
  }

}
