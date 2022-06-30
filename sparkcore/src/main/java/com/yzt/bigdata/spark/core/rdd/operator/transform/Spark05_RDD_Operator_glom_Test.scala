package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_glom_Test {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 glom

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //List => Int
    //Int => Array
    val glomRDD = rdd.glom()
    val maxRDD = glomRDD.map(
      array => {
        array.max
      }
    )
    println(maxRDD.sum())

    sc.stop()
  }
}
