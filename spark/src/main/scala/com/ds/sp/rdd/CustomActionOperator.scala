package com.ds.sp.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * @Package: com.ds.sp.rdd
 * @Description: TODO
 * @Create on: 2024/12/30 16 21
 * @Author: devil
 * @version: v1.0.0
 * */
object CustomActionOperator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("custom action")
    val sc = new SparkContext(conf)

    val list = sc.makeRDD(Array(3,4,1, 2, 8,9, 7, 5,6),3)
      .filter(_ > 2 )
    sc.runJob(list,myAction _)
      .foreach(println)

  }

  /**
   * 在driver端获取executor执行的task结果，比如task是一个规则，那么想看下有多少条数据命中规则
   * @param iterator
   */
  def myAction(iterator: Iterator[Int]) = {
    var count = 0
    iterator.foreach{
      each => count += 1
    }
    (TaskContext.getPartitionId(),count)
  }

}
