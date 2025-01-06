package com.ds.sp.rdd.partitioner

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.partitioner
 * @Description: TODO
 * @Create on: 2025/1/2 21 08
 * @Author: devil
 * @version: v1.0.0
 * */
object HashPartitioner01 {
  def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf()
        .setAppName("hash Partitioner").setMaster("local")
      val sc = new SparkContext(conf)

      val counts = sc.parallelize(List((1, 'a'), (1, 'a'), (2, 'b'), (2, 'bb), (3, 'c')), 3)
        .partitionBy(new HashPartitioner(3))

    }


}
