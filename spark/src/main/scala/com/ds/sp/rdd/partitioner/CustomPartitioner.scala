package com.ds.sp.rdd.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.partitioner
 * @Description: TODO
 * @Create on: 2025/1/3 09 11
 * @Author: devil
 * @version: v1.0.0
 * */
object CustomPartitioner {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("custom partitioner")

    val sc = new SparkContext(conf)

    sc.makeRDD(List(("勇士","Curry"),("湖人","Kobe"),("火箭","YaoMing"),("掘金","Jokic")))
      .partitionBy(new MyPartitioner(3))
      .mapPartitionsWithIndex{
        (id,iter) =>{
          println(s"分区号：${id},数据：${iter.mkString(",")}" )
          iter
        }
      }
      .collect()

  }

}
class MyPartitioner(num:Int)  extends  Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    key match {
      case "勇士" => 0
      case "湖人" => 1
      case _ => 2
    }
  }

}


