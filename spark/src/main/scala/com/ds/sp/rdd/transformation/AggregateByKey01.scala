package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 11 01
 * @Author: devil
 * @version: v1.0.0
 * 函数详解：
将数据根据不同的规则进行分区内计算和分区间计算 ，比如取出每个分区内相同  key  的最  大值然后分区间相加。需要注意的是：aggregateByKey 最终的返回数据结果应该和初始值的类
型保持一致

// aggregateByKey 算子是函数柯里化 ，存在两个参数列表
// 1. 第一个参数列表中的参数表示初始值
// 2. 第二个参数列表中含有两个参数
//   2.1 第一个参数表示分区内的计算规则 //   2.2 第二个参数表示分区间的计算规则


备注 1 ：当分区内计算规则和分区间计算规则相同时 ，aggregateByKey  就可以简化为
foldByKey
备注 2：aggregateByKey 的初始值只提供给分区内做计算 ，注意区别 aggregate
 * */
object AggregateByKey01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("aggregateByKey")

    val sc = new SparkContext(conf)
    sc.makeRDD(List( ("a",1), ("a",2), ("c",3), ("b",4), ("c",5), ("c",6)), 2)
      .mapPartitionsWithIndex{
        (id,iter) =>{
          var list =  List[(String,Int)]()
          while (iter.hasNext){
             var next = iter.next()
            list = list :+ next
            print(id + "分区: " + next)
          }
          println()
          list.iterator
        }
      }
      //.aggregateByKey(0)(_ + _ ,  _ + _)
      .aggregateByKey(0) (
        {
              //分区内求最大值
          (v1,v2) =>math.max(v1,v2)
        }
        ,
        {
              //分区间相加
          (v1,v2) =>v1+v2
        }
      )
      .collect()
      .foreach(println)

  }


}
