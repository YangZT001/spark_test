package com.yzt.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//广播变量：放在一个executor的共享内存中，允许不同的task访问，但不允许修改
object Spark06_Bc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3),("d", 6)
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6)
    ))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    //封装广播变量
    val bc = sc.broadcast(map)

    val newRDD = rdd1.map {
      case (w, c) =>{
        val l = bc.value.getOrElse(w, 0)
        (w,(c,l))
      }
    }
    newRDD.collect().foreach(println)

    sc.stop()
  }
}
