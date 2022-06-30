package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_cogroup {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 cogroup

    //connect+group
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3),("c",4)
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5)//, ("c", 6)
    ))
    val cogroup = rdd1.cogroup(rdd2)
    cogroup.collect().foreach(println)



    sc.stop()
  }
}
