package com.yzt.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //获取系统累加器
    //spark默认提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")
    /*
    sc.longAccumulator
    sc.doubleAccumulator
    sc.collectionAccumulator
     */
    rdd.foreach(
      num=>{
        sumAcc.add(num)
      }
    )
    println(sumAcc.value)
    sc.stop()
  }
}
