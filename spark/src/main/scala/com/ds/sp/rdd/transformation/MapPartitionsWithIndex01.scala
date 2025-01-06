package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/27 17 43
 * @Author: devil
 * @version: v1.0.0
 * */
object MapPartitionsWithIndex01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("mapPartitionsWithIndex")
    val sc = new SparkContext(conf)
    sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
      .mapPartitionsWithIndex{
        (partitionId,iter) => {//分区ID，分区数据
          var result = List[Int]()
          var even = 0
          var odd = 0
          while (iter.hasNext) {
            val value = iter.next()
            if (value % 2 == 0) {
              even += value
              println(s"partitionId: ${partitionId},value：${value},even: ${even}")
            } else {
              odd += value
              println(s"partitionId: ${partitionId},value：${value},odd: ${odd}")
            }

          }
          result = result :+ even :+ odd
          result.toIterator
        }
      }
      .collect()
      .foreach(println)
  }

}
