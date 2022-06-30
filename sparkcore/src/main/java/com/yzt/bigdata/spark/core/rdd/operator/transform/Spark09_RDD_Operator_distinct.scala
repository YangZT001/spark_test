package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_distinct {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 distinct
    //去重
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
    val rdd1 = rdd.distinct()
    rdd1.collect().foreach(print)
    sc.stop()
  }
}
