package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_filter {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 filter
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    rdd.filter(_%2==1).collect().foreach(println)
    sc.stop()
  }
}
