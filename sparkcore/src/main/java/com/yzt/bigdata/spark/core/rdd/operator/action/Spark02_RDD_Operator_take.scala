package com.yzt.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_take {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkconf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    //TODO 算子 take
    //取个数
    val i = rdd.take(2)
    println(i.mkString(","))

    sc.stop()
  }
}
