package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Create on: 2024/12/30 10 23
 * @Author: devil
 * @version: v1.0.0
 * */
object Save01 {
  def main(args: Array[String]): Unit = {
    /*System.setProperty("HADOOP_HOME", "D:\\env\\hadoop-3.1.0")
    System.setProperty("hadoop.home.dir", "D:\\env\\hadoop-3.1.0")
    println("HADOOP_HOME: " + System.getProperty("HADOOP_HOME"))
    println("hadoop.home.dir: " + System.getProperty("hadoop.home.dir"))*/
    val conf = new SparkConf().setMaster("local").setAppName("Save Demo")
    val sc = new SparkContext(conf)

   /* val path = this.getClass.getResource("/data/abc.txt").getPath
    sc.textFile(path)
      .collect()
      .foreach(println)*/

   // println("HADOOP_HOME: " + System.getenv("HADOOP_HOME"))
    sc.makeRDD(List(1,2,3,4,5,6,7),2)
      .saveAsTextFile("spark/src/main/resources/data/abcdw.txt")
      //.saveAsTextFile("hdfs://ds-bigdata-001:8020/user/zhting241208/spark_datas/abcdw.txt")

  }
}
