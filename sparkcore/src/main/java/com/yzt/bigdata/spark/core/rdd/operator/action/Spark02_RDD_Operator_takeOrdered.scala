package com.yzt.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_takeOrdered {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkconf)

    val rdd = sc.makeRDD(List(4,2,3,1))

    //TODO 算子 takeOrdered
    //先排序再取个数
    val i = rdd.takeOrdered(3)
    println(i.mkString(","))

    sc.stop()
  }
}
