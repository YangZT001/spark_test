package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_value_value1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 双value
    val rdd1 = sc.makeRDD(List(1,2,3,4),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),4)

    //分区数量不同将会报错
    //Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    val rdd6 = rdd1.zip(rdd2)
    println("拉链(T,T):",rdd6.collect().mkString(","))

    sc.stop()
  }
}
