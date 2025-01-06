package com.ds.sp.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package: com.ds.sp.rdd.transformation
 * @Create on: 2024/12/28 11 43
 * @Author: devil
 * @version: v1.0.0
 * */
object CombineByKey01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("combineByKey")
    val sc = new SparkContext(conf)
// 已有一个(性别，姓名)二元组，现在需要统计每种性别下的姓名和人数
//最终想要的格式如下：("male",(List("Mobin","Kpop","Lufei"),3))
//("female",(List("Lucy","Amy"),2))
    val people = List(("male", "Mobin"), ("male", "Kpop"), ("female", "Lucy"), ("male", "Lufei"), ("female", "Amy"))

    sc.makeRDD(people,2)
      .mapPartitionsWithIndex{
        (id,iter) =>{
          var res = List[(String,String)]()
          while(iter.hasNext){
            val next = iter.next()
            print(id + "分区：" + next)
            res :+= next
          }
          println()
          res.iterator
        }
      }
      //combine的第一个参数解释：将已知的value类型转换为目标value类型
      //第二个参数解释：分区内聚合（目标value类型，当前value元素类型）
      //第三个参数解释：分区间聚合
      .combineByKey(
        (v:String) =>(List(v),1), //遇到第一次新key时才会创建（List(Mobin,1)）
            //下面的第一个参数相当于上一步创建的结果类型，第二个参数即相同key的value值
            //List("Mobin",1)）,Kpop
        (c1:(List[String],Int),v2:String) => (v2::c1._1,c1._2+1),  //(List("Mobin","Kpop"),2)
        (c1:(List[String],Int),c2:(List[String],Int)) => (c1._1 ::: c2._1,c1._2+c2._2)
      )
      .collect()
      .foreach(println)

  }

}
