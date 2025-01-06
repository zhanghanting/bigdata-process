package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 10 48
 * @Author: devil
 * @version: v1.0.0
 * 函数详解：
*可以将数据按照相同的 Key 对 Value 进行聚合，作用就是对相同 key 的数据进行处理，最 终每个 key 只保留一条记录。
*函数语法：
*def reduceByKey(func: (V, V) = > V): RDD[(K, V)]
*def reduceByKey(func: (V, V) = > V, numPartitions: Int): RDD[(K, V)]
 * */
object ReduceByKey01{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val dataRDD1 = sc.makeRDD(List(("a",1),("a",10),("b",2),("c",3)),2)

    dataRDD1.reduceByKey(_+_)
      .collect()
      .foreach(println)

    dataRDD1.reduceByKey(_ + _,3)
      .collect()
      .foreach(println)
  }
}
