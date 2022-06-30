package com.yzt.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_fold {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkconf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //TODO 算子 fold
    //分区计算
    //分区内和分区间计算相同时
    val result = rdd.fold(0)(_+_)
    println(result)

    sc.stop()
  }
}
