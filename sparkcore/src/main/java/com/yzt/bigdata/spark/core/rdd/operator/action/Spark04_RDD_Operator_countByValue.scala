package com.yzt.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_countByValue {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkconf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",2),("b",2)
    ))

    //TODO 算子 countByValue
    //分区计算
    //分区内和分区间计算相同时
    val result = rdd.countByValue()
    println(result)

    sc.stop()
  }
}
