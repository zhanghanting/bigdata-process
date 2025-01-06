package com.ds.sp.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/27 17 23
 * @Author: devil
 * @version: v1.0.0
 * 函数详解：
 * mapPartitions 的输入函数应用于每个分区，也就是把每个分区中的内容作为整体来处理的。 如果在映射的过程中需要频繁创建额外的对象 ，
 * 使用 mapPartitions 要比 map 高效的过。参数  preservesPartitioning 表示是否保留父 RDD 的 partitioner 分区信息。
 *
 * 思考题： map 和  mapPartitions  的区别？ 数据处理角度：
Map 算子是分区内一个数据一个数据的执行，类似于串行操作。而  mapPartitions 算 子是以分区为单位进行批处理操作。
功能的角度：
Map 算子主要目的将数据源中的数据进行转换和改变。但是不会减少或增多数据。

MapPartitions 算子需要传递一个迭代器，返回一个迭代器，没有要求的元素的个数保持不
变 ， 所以可以增加或减少数据 性能的角度:
Map 算子因为类似于串行操作，所以性能比较低，而是  mapPartitions 算子类似于批 处理 ，所以性能较高。但是 mapPartitions 算子会长时间占用内存 ，那么这样会导致内存  可能不够用 ， 出现内存溢出（ OOM） 的错误。所以在内存有限的情况下 ，不推荐使用。
举例说明：
比如我们对一个含有 100 条 log 数据的分区进行操作，使用 map 的话函数要执行 100 次计算。使用 MapPartitions 操作之后，一个 task 仅仅会执行一次 function，function 一 次接收所有的 partition 数据。如果 map 执行的过程中还需要创建对象 ，比如创建 redis 连 接，jdbc 连接等。map 需要为每个元素创建一个链接而 mapPartition 为每个 partition 创 建一个链接。

             开发怎么用 ：企业中 ，如果我们需要在 map 处理完成之后 ，需要通过 JDBC 的方式写入其他存 储（ MySQL）,会使用 mapPartitions 的方式写入
 * */
object MapPartitions01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("mapPartition")
    val sc = new SparkContext(conf)
    sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
      .mapPartitions{
        iter =>{// 分区中的所有数据
          var result = List[Int]()
          var even = 0
          var odd = 0
          while (iter.hasNext){
            val value = iter.next()
            if(value % 2 == 0) {
              even += value
              println(s"partitionId: ${TaskContext.getPartitionId()},value：${value},even: ${even}")
            } else {
              odd += value
              println(s"partitionId: ${TaskContext.getPartitionId()},value：${value},odd: ${odd}")
            }

          }
          result = result :+ even :+ odd
          result.toIterator
        }
      }
      .collect()
      .foreach(println)

  }

}
