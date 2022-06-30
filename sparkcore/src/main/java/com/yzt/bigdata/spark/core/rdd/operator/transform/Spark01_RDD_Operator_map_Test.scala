package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_map_Test {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子map
    val rdd = sc.textFile("datas/apache.log")

    //长字符串=》短字符串
    val mapRDD = rdd.map(_.split(" ")(6))
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
