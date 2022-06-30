package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_mapPartitionsWithIndex1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(
          num=>{
            (index,num)
          }
        )
      }
    )
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}

