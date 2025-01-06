package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/27 17 56
 * @Author: devil
 * @version: v1.0.0
 * 函数详解：
*将同一个分区的数据直接转换为相同类型的内存数组进行处理 ，
*分区不变。如果 flatten 理 解做了一次列转行 ，而 glom 就是一次行转列（将多行一列数据转为一行多列显示）
 *
 *
 * 开发怎么用 ：计算所有分区最大值求和（分区内取最大值 ，分区间最大值求和）
 * */
object Glom01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("glom")
    val sc = new SparkContext(conf)
    sc.makeRDD(List(1, 2, 3, 4,5,6), 2)
      .glom()
      .collect()
      .foreach {
        arr => {
          arr.foreach(print)
          println()
        }
      }
  }

}
