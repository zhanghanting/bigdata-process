package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 10 12
 * @Author: devil
 * @version: v1.0.0
 * */
object ZipPartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setAppName("zipPartition").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5),2)
    val rdd2 = sc.makeRDD(Seq("A", "B", "C", "D", "E"), 2)
    // 查看rdd1的分区信息
     rdd1.mapPartitionsWithIndex {
      (index, iter) => {
        var result = List[String]()
        while (iter.hasNext) {
          result = result :+ ("part_" + index + "|" + iter.next()) // 修复语法错误
        }
        result.iterator
      }
    }.collect()
       .foreach(println)

    //查看rdd2的分区信息
    rdd2.mapPartitionsWithIndex {
      (index, iter) => {
        var result = List[String]()
        while (iter.hasNext) {
          result = result :+ ("part_" + index + "|" + iter.next()) // 修复语法错误
        }
        result.iterator
      }
    }.collect()
      .foreach(println)

    rdd1.zipPartitions(rdd2){
      (rdd1Iter, rdd2Iter) => {
        var result = List[String]()
        while (rdd1Iter.hasNext && rdd2Iter.hasNext) {
          result ::= (rdd1Iter.next() + "_" + rdd2Iter.next())
        }
        result.iterator
      }
    }.collect()
      .foreach(println)
  }


}
