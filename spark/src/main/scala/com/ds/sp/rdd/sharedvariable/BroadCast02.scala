package com.ds.sp.rdd.sharedvariable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.sharedvariable
 * @Create on: 2025/1/3 16 42
 * @Author: devil
 * @version: v1.0.0
 * */
object BroadCast02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("broadcast demo")
    //.set("spark.local.dir", "spark/src/main/resources/data")

    val sc = new SparkContext(conf)

    val zhangsan = new Stu2(1,"zhangsan")
    val bc01: Broadcast[Stu2] = sc.broadcast(zhangsan)

    // 这里广播变量发生改变的原因是广播出去的是对象的地址值
    zhangsan.name = "lisi"

    sc.makeRDD(List(1,2,3,4,5))
      //.foreach( num => println(a * num))
      .foreach( num => println(bc01.value))

    println(bc01.value)
  }

}

class Stu2(var id:Int,var name:String) extends Serializable {
  override def toString: String = s"id:${id},name:${name}"
}
