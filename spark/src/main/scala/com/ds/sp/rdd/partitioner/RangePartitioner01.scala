package com.ds.sp.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}
import org.json4s.scalap.scalasig.ScalaSigEntryParsers.index

/**
 * @Package: com.ds.sp.rdd.partitioner
 * @Description: TODO
 * @Create on: 2025/1/2 21 09
 * @Author: devil
 * @version: v1.0.0
 * */
object RangePartitioner01 {
  def main(args: Array[String]): Unit = {
    //创建配置文件
    val conf: SparkConf = new
        SparkConf().setAppName("rangePartitioner").setMaster("local[*]") //创建 SparkContext,该对象是提交的入口
    val sc = new SparkContext(conf) //创建 RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "aaa"), (2, "bbb"), (3, "ccc"),(3, "ccc2"),(3, "cc3"),(4, "dd")), numSlices = 3)
    //先按照分区打印分布
    rdd.mapPartitionsWithIndex {
      (index, datas) => {
        println(index + "--- >" + datas.mkString(","))
        datas
      }
    }.collect()
    println("********************************************")

    //方式 1  采用默认分区器
    val newRDD1: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner (2))
    //查看分区后的数据分布
    newRDD1.mapPartitionsWithIndex {
      (index, datas) => {
        println(index + "--- >" + datas.mkString(","))
        datas
      }
    }.collect()
    println("********************************************")


    //方式 2 采用 RangerPartitioner
    val newRDD2: RDD[(Int, String)] = rdd.partitionBy(new RangePartitioner(3, rdd))
    newRDD2.mapPartitionsWithIndex {
      (index, datas) => {
        println(index + "--- >" + datas.mkString(","))
        datas
      }
    }.collect()
  }

}
