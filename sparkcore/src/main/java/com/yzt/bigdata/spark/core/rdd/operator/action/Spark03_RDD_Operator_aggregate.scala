package com.yzt.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_aggregate {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkconf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //TODO 算子 aggregate
    //分区计算
    //aggregatebyKey:初始值只会参与分区内计算
    //aggregate:初始值会参与所有计算
    val result = rdd.aggregate(0)(_+_,_+_)
    println(result)

    sc.stop()
  }
}
