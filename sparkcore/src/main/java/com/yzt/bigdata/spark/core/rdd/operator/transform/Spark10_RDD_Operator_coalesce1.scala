package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_coalesce1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 coalesce 缩减/扩大 分区
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //扩大分区，必须shuffle
    //spark提供一个简化的操作
    //缩减：coalesce,如果想要数据均衡,则shuffle
    //扩大：repartition,底层就是coalesce
//    val newRDD = rdd.coalesce(3,true)
    val newRDD = rdd.repartition(3)
    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
