package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_sortBy1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 sortBy
    val rdd = sc.makeRDD(List(
      ("1",1),("11",2),("2",3)
    ),2)
    //默认情况不改变分区，但中间存在shuffle操作
    val newRDD = rdd.sortBy(_._1.toInt,false)
    newRDD.collect().foreach(println)

    sc.stop()
  }
}
