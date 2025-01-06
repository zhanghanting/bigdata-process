package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Create on: 2024/12/27 17 17
 * @Author: devil
 * @version: v1.0.0
 *
 *           函数详解：
 *           map 是对 RDD 中的每个元素都执行一个指定函数来产生一个新的 RDD。任何原 RDD 中的 元素在新 RDD 中都有且只有一个元素与之对应
 *           将处理的数据逐条进行映射转换 ，这里的转换 可以是类型的转换 ，也可以是值的转换。
 *
 *           开发怎么用：企业中一般需要对数据做数据清洗的操作，
 *           一般会使用 RDD.map 的方式对每一行 数据做校验和清洗
 * */
object Map_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1, 2, 3, 4, 5))
      .map(_ * 3)
      .collect()
      .foreach(println)
  }

}
