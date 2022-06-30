package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_map {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子map
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

//    def mapFunction(num: Int):Int={
//      num*2
//    }
//    val value = rdd.map(mapFunction)
    val value = rdd.map(_ * 2)
    val array = value.collect()
    array.foreach(println)
    sc.stop()
  }
}
