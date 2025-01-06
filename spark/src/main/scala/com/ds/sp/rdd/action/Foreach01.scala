package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/28 16 38
 * @Author: devil
 * @version: v1.0.0
 * */
object Foreach01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local[2]").setAppName("foreach demo")
    val sc = new SparkContext(conf)
    val value = sc.makeRDD(List(1,2,3,4,5,6,7,8), 2).map(_ * 2)
    //collect是在driver端执行的
    value.collect().foreach(println)
    println ("**********")
    //foreach是在每个executor上执行的
    value.foreach(println)


    val value2 = sc.makeRDD(List(1, 2, 3, 4), 4)

    /*
    * sum =0 会传递到两个 Executor 中 ，进行累加 ，两个 Executor 的 sum 各自	 累加自己的数据 ，
    * 但是累加结束的 sum 的值并没有回调给 Driver 中 ，即用户自定义的普通变量
    * 是没有办法返回给 Driver 端的，因此，Driver 中的sum 一直是 0。
    * */
    var sum = 0
    value2.foreach(num => {
      sum += num
    })
    println("总和为： " + sum)


    /*
    * 为了将累加的结果返回给 driver， spark 提供了一个特殊的数据结构叫做累加器
    *
    * */
    val sum2 = sc.longAccumulator("sum")
    value2
    .foreach(num => {
      sum2.add(num)
    })
    println("总和为： " + sum2.value)
  }

}
