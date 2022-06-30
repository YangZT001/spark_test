package com.yzt.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //分区内计算，分区间计算
    //val i = rdd.reduce(_+_)
    //println(i)

    /* 结果sum = 0
    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println(sum)
    */
    sc.stop()
  }
}
