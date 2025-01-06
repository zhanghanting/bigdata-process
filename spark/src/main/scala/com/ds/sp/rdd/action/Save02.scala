package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Description: TODO
 * @Create on: 2024/12/30 16 03
 * @Author: devil
 * @version: v1.0.0
 * */
object Save02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName( "savedemo").setMaster("local")
    val sc = new SparkContext(conf)

    // 企业中一般会使用此种方式
    val path = this.getClass.getResource("spark/src/main/resources/data/abc.txt").getPath

    // 一般测试中会使用此种方式
    sc.textFile("spark/src/main/resources/data/abc.txt").repartition(2)
      // 上一步有几个分区，就会有几个数据文件。另外下面保存的路径是会生成 文件夹
      // 如果指定位置已经存在 ，则会报错
      .saveAsTextFile("spark/src/main/resources/data/aaaa2.txt")



      // 如果是一个目录 ，会读取目录下除“ . ”和“_”开头的所有文件 sc.textFile("src/main/resources/data/aaaa1.txt")
      //.collect()
      //.foreach(println)

    // 以“ . ”和“_”开头文件会被认为不存在
    //       sc.textFile("src/main/resources/data/a bcd1.txt/_acc.txt") //          .collect()
    //          .foreach(println)

    // 这里之所以能够访问到 hdfs，是因为本地配置了 HADOOP_HOME，如下： // println(System.getenv("HADOOP_HOME"))
    // 如果本地没有配置 HADOOP_HOME ，可以将 core-site.xml 和 hdfs-site.xml 放在 resources 目录下，
    // 但是一旦放在 resourcer 目录下 ，这个时候默认的路径又变成了 hdfs:///， 本地的话需要加 file:///

      // sc.textFile("src/main/resources/data/aaaa1.txt") sc.textFile("file:///" + path)
      // 即使是上面报错了 ，下面依旧可以在 hdfs 生成文件夹 ，只不过是空的， 这点区别于本地
      //.saveAsTextFile("hdfs://hadoop004:8020/spark/whj2333")



    // 一般在生产环境中不建议直接连接 hdfs 集群 ，而是通过 args 获取指定传参
    //，然后通过 spark -submit 将指定的位置传入 // sc.textFile(args(0)).saveAsTextFile(args(1))
      sc.textFile("hdfs://hadoop004:8020/spark/whj1").collect()
        .foreach(println)
  }

}
