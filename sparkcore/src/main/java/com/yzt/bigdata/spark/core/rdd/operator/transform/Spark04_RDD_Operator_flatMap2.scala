package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_flatMap2 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 flatMap

    val rdd = sc.makeRDD(List(
      List(1,2),3,List(4,5)
    ))
    val flatRDD = rdd.flatMap {
      case list: List[_] => list
      case dat => List(dat)
    }
    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
