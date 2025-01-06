package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/27 22 41
 * @Author: devil
 * @version: v1.0.0
 * */
object SortByKey01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sortByKey")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array((1,"A"),(3,"D"),(2,"B"),(4,"C")),2)
      .mapPartitionsWithIndex {
        (id, iter) => {
          var list =  List[(Int,String)]()
          while (iter.hasNext){
            val next = iter.next()

            list = list :+ next
            print(id + "分区前：" + next)

          }
          println()
          list.iterator
        }
      }
      .sortByKey() //经验证后sortByKey是全局排序
      .mapPartitionsWithIndex{
        (id, iter) => {
          var list = List[(Int, String)]()
          while (iter.hasNext) {
            val next = iter.next()

            list = list :+ next
            print(id + "分区后：" + next)

          }
          println()
          list.iterator
        }
      }
      .collect()
      .foreach(println)
  }

}
