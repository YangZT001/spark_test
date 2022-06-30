package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_map_Par {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子map
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val mapRDD0 = rdd.map(
      n => {
        println("num0=",n)
        n
      }
    )
    val mapRDD1 = mapRDD0.map(
      n => {
        println("num1=",n)
        n
      }
    )
    mapRDD1.collect()
    sc.stop()
  }
}
