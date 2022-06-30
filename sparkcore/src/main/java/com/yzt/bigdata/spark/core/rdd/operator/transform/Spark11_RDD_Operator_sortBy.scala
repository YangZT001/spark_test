package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_sortBy {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 sortBy
    val rdd = sc.makeRDD(List(1,2,5,4,3,6),2)
    rdd.saveAsTextFile("output")
    val newRDD = rdd.sortBy(n => n)
    newRDD.saveAsTextFile("output1")

    sc.stop()
  }
}
