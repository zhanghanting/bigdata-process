package com.ds.sp.rdd.persistandcache

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.persistandcache
 * @Description: TODO
 * @Create on: 2025/1/2 15 41
 * @Author: devil
 * @version: v1.0.0
 *
 * 需要特别注意的是，缓存可以将RDD的partition持久化到磁盘，但该  partition 对  应的缓存文件由 Spark 的 Block Manager管理。
 * 一旦driver 执行结束 ， Block Manager也会stop，被cache到磁盘上的RDD也会被清空（整个block Manager使用的local文件夹被删除） 。
 * */
object Cache01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("cache demo")
      .set("spark.local.dir","spark/src/main/resources/data")
    val sc = new SparkContext(conf)

    val cacheRDD = sc.textFile("spark/src/main/resources/data/gp.txt")
      .filter(_.startsWith("A"))
     // .cache()
      .persist(StorageLevel.DISK_ONLY)
    //产生了第一次Action操作，产生了一个job
    println(cacheRDD.count())

    //触发第二次Action操作，又产生了一个job
    println(cacheRDD.count())



    Thread.sleep(100000)
  }

}
