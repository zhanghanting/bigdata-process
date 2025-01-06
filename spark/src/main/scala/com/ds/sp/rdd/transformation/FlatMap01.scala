package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/27 17 47
 * @Author: devil
 * @version: v1.0.0
 *
 * map的基础上扁平化，列转行
 *
 * 开发怎么用 ：flatMap 非常适合用于扁平化处理 ，可以将 RDD 中的每一个元素转换为多个新的 元素 ，并组成新的 RDD。具体应用场景如下：
单词拆分 ：在文本数据处理中 ， Spark FlatMap 可以用来将每一行文本拆分为单独的单词。 扁平化聚合： Spark FlatMap 可以用来聚合嵌套的数据结构 ，例如将一组数组转换成一个大 的数组。
 * */
object FlatMap01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("flatmap")
    val sc = new SparkContext(conf)
    sc.parallelize(Array("hello world","hello spark"))
      .flatMap(_.split(" "))
      .collect()
      .foreach(println)
  }

}
