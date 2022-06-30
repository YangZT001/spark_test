package com.yzt.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_filter_Test {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 filter
    val rdd = sc.textFile("datas/apache.log")
    rdd.filter(
      line => {
        val datas = line.split(" ")(3)
        datas.startsWith("17/05/2015")
      }
    ).collect().foreach(println)
    sc.stop()
  }
}
