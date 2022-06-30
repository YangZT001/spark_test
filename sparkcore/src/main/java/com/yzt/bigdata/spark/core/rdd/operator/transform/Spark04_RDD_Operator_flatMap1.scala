package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_flatMap1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 flatMap
    val rdd = sc.makeRDD(List(
      "Hello Scala", "Hello Spark"
    ))
    val flatRDD = rdd.flatMap(_.split(" "))
    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
