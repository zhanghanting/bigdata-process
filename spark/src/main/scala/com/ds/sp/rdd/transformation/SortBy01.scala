package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Description: TODO
 * @Create on: 2024/12/28 09 25
 * @Author: devil
 * @version: v1.0.0
 * */
object SortBy01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sortBy")
    val sc = new SparkContext(conf)
    val students = List(
      Student("jack", 20),
      Student("susan", 18),
      Student("zhangsan", 22),
      Student("lisi", 23),
      Student("mike", 22)
    )

    sc.makeRDD(students)
     // .sortByKey()
     // .sortBy(_.age)(Ordering[Int].reverse,ClassTag.Int)
      .sortBy(-_.age)
      .collect()
      .foreach(println)
  }

}
/*class Student(var name:String,var age:Int) extends Ordered[Student] with Serializable
{
  override def compare(that:Student):Int = {
    //升序
    var res = this.name.compareTo(that.name)
    if(res == 0){
      //降序
      res = that.age - this.age
    }
    res
  }

  override def toString: String = s"Student name is ${name},age is ${age}"
}*/
case class Student(var name:String,var age:Int)