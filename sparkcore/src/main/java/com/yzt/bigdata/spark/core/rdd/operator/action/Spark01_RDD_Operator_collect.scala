package com.yzt.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_collect {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkconf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    //TODO 算子 collect
    rdd.collect()


    sc.stop()
  }
}
