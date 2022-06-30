package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_flatMap {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 flatMap
    var rdd = sc.makeRDD(List(
      List(1,2),List(3,4)
    ))
    val flatRDD = rdd.flatMap(
      List => List
    )
    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
