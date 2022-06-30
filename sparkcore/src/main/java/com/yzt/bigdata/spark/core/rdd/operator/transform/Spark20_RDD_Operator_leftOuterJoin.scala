package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator_leftOuterJoin {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 leftOuterJoin
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5)//, ("c", 6)
    ))

    val leftjoinRDD = rdd1.leftOuterJoin(rdd2)
    val rightjoinRDD = rdd1.rightOuterJoin(rdd2)
    leftjoinRDD.collect().foreach(println)
    rightjoinRDD.collect().foreach(println)




    sc.stop()
  }
}
