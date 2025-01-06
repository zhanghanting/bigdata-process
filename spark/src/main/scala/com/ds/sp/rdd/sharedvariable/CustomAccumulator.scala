package com.ds.sp.rdd.sharedvariable

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import java.util

/**
 * @Package: com.ds.sp.rdd.sharedvariable
 * @Description: TODO
 * @Create on: 2025/1/4 09 33
 * @Author: devil
 * @version: v1.0.0
 * */
object CustomAccumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("custom accumulator demo")

    val sc = new SparkContext(conf)

    val wordAcc = new MyAccumulator
    sc.register(wordAcc,"wordAcc")

    //具体业务逻辑
    sc.textFile("spark/src/main/resources/data/wc.txt")
      .flatMap(_.split(" "))
      .foreach(word => wordAcc.add(word))

    println(wordAcc.value)
  }

}
class MyAccumulator extends AccumulatorV2[String,java.util.HashMap[String,Int]]{
  private val map = new util.HashMap[String,Int]()

  override def isZero: Boolean = map.isEmpty

  //在所有的executor的task中都保留一份单独的累加器
  override def copy(): AccumulatorV2[String, util.HashMap[String, Int]] = new MyAccumulator

  //累加器重置
  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if(map.containsKey(v)){
      map.put(v,map.get(v)+1)
    }else{
      map.put(v,1)
    }
  }

  //将所有task处理完的累加器数据一起合并
  override def merge(other: AccumulatorV2[String, util.HashMap[String, Int]]): Unit = {
    other match {
      case o: MyAccumulator => {
        val set = o.value.entrySet()
        set.forEach{
          entry => {
            if(map.containsKey(entry.getKey)){
              map.put(entry.getKey,map.get(entry.getKey)+entry.getValue)
            }else{
              map.put(entry.getKey,entry.getValue)
            }
          }
        }
      }
      case _ => throw new UnsupportedOperationException(s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: util.HashMap[String, Int] = map
}