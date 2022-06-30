package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_join {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 join
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("a", 6)
    ))
    //两个数据源中有相同的key，会将value连在一起形成元组
    //key没有匹配则不会出现结果
    //多个匹配结果则全部匹配
    val joinRDD = rdd1.join(rdd2)
    joinRDD.collect().foreach(println)




    sc.stop()
  }
}
