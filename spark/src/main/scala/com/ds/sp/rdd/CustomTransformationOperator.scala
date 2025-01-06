package com.ds.sp.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}

/**
 * @Package: com.ds.sp.rdd
 * @Description: TODO
 * @Create on: 2024/12/28 15 50
 * @Author: devil
 * @version: v1.0.0
 * 自定义transformer算子
 * */
object CustomTransformationOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custom Transformation").setMaster("local")
    val sc = new SparkContext(conf)

    val inputData: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    val transformedData = new MyTransformationRDD(inputData)
    transformedData.collect().foreach(println)
  }

}
class MyTransformationRDD(prev: RDD[Int]) extends RDD[Int](prev) { override def compute(split: Partition, context: TaskContext):
Iterator[Int] = {
  val inputIterator = firstParent[Int].iterator(split, context)
  inputIterator.map(_ * 2)
}

  override protected def getPartitions: Array[Partition] =
    firstParent[Int].partitions }
