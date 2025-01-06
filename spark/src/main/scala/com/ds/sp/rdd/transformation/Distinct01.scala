package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: 函数详解：
将数据集中重复的数据去重 ，属于全局去重 ，即每个分区放在一起都没有重复的数据。有两
种不同的实现方式 ，一种使用默认的父 rdd 的分区 ，不会产生 shuffle；另外一种是去重后的数
据被重新分配到各个分区 ，说明此过程有 shuffle
 * @Create on: 2024/12/27 21 32
 * @Author: devil
 * @version: v1.0.0
 * */
object Distinct01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("distinct")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array(1,2,3,4,3,4,2,5,6,7,6),2)
      //.distinct() //distinct是全局去重
      .distinct(3) //去重后可能造成数据倾斜问题，可以指定分区数据对数据重分配
      .mapPartitionsWithIndex{
        (id,iter) =>{
          println(id + "分区: " + iter.mkString(","))
          iter
        }
      }
      .collect()
      .foreach(println)

  }

}
