package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_coalesce {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 coalesce 缩减分区
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //coalesce默认情况下不会将分区数据打乱重新组合
    //这种情况下的缩减分区会导致数据倾斜
    //如果想要数据均衡，则进行shuffle处理（打乱重新组合）
    val newRDD = rdd.coalesce(2,true)
    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
