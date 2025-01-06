package com.ds.sp.rdd.sharedvariable

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.sharedvariable
 * @Create on: 2025/1/3 16 42
 * @Author: devil
 * @version: v1.0.0
 * */
object BroadCast01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("broadcast demo")
    //.set("spark.local.dir", "spark/src/main/resources/data")

    val sc = new SparkContext(conf)

    var a = 3
    val bc01 = sc.broadcast(a)
    a = 4
    sc.makeRDD(List(1,2,3,4,5))
      //.foreach( num => println(a * num))
      .foreach( num => println(bc01.value * num))

    println(bc01.value)
  }

}

class Stu(id:Int,name:String) extends Serializable {
  override def toString: String = s"id:${id},name:${name}"
}
