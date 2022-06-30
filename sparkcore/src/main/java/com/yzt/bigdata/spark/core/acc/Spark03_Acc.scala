package com.yzt.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //获取系统累加器
    //spark默认提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")
    val mapRDD = rdd.map(
      num => {
        sumAcc.add(num)
        num
      }
    )
    //累加器少加：转换算子中添加累加器，若没有行动算子，则不执行
    println(sumAcc.value)
    //累加器多加：多次执行行动算子，将会多加结果
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)
    sc.stop()
  }
}
