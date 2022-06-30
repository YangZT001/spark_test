package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Operator_groupByKey {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 reduceByKey
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("b",4)
    ))
    val groupRDD = rdd.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
