package com.ds.sp.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.action
 * @Create on: 2024/12/28 21 22
 * @Author: devil
 * @version: v1.0.0
 * */
object TakeOrdered01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local").setAppName("take demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(List(1,0,7, 3, 4, 5, 6), 3)
      //.takeOrdered(4)
      .top(4)//降序
      .foreach(println)

    var girls = List(Girl("Alice",12,56),Girl("Alice",14,56),Girl("nana",27,56),Girl("qiqi",22,45),Girl("susan",18,57))

    val first = sc.makeRDD(girls)
      // .takeOrdered(2)(Ordering.by[Girl,String](_.name))
      //.takeOrdered(2)(Ordering.by[Girl,Int](_.age))
      // .takeOrdered(2)(Ordering.fromLessThan[Girl](_.age < _.age))
      //.takeOrdered(2)
      .first()
    println(first)
  }

}
case class Girl(name:String,age:Int,weight:Int) extends Ordered[Girl]{
  override def compare(that: Girl): Int = {
    var result = this.name.compareTo(that.name)
    if(result == 0){
      result = this.age - that.age
    }
    result
  }
  override def toString: String = s"name is ${name},age is ${age},weight is ${weight}"
}
