package com.yzt.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
//广播变量
object Spark05_Bc {
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
    //join会导致数据量几何增长，影响shuffle
    //val newRDD = rdd1.join(rdd2)

    val newRDD = rdd1.map {
      case (w, c) =>{
        val l = map.getOrElse(w, 0)
        (w,(c,l))
      }
    }
    newRDD.collect().foreach(println)

    sc.stop()
  }
}
