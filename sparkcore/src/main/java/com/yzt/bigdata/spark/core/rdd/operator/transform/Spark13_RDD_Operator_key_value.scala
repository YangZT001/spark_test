package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark13_RDD_Operator_key_value {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 key-value
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD = rdd.map((_,1))
    //根据指定的分区规则对数据进行重分区
    val newRDD = mapRDD.partitionBy(new HashPartitioner(2))
    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
