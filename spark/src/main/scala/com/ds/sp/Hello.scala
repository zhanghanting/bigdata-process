package com.ds.sp

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds
 * @Description: TODO
 * @Create on: 2024/12/26 22 26
 * @Author: devil
 * @version: v1.0.0
 * */
object Hello {
  def main(args: Array[String]): Unit = {
    //println(System.getenv("HADOOP_HOME"))
    val conf: SparkConf = new SparkConf()
      //.set("spark.hadoop.validateOutputSpecs", "false")
      .setMaster("local")
      .setAppName("hello spark")
    val context = new SparkContext(conf)

    val rdd = context.parallelize(List(1, 2, 3, 4, 5),2).map(_ * 3)
    val collect = rdd.filter(_ > 10).collect()
    println(rdd.reduce(_+_))

    for(arg <- collect){
      println(arg + ">>")
    }

  }

}
