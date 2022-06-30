package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_aggregateByKey1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 aggregateByKey
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)
    ),2)
    val aggregateRDD = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )
    aggregateRDD.collect().foreach(println)

    //若分区间和分区内的计算规则相同，spark提供了简单的方法
    val foldRDD = rdd.foldByKey(0)(_ + _)
    foldRDD.collect().foreach(println)
    sc.stop()
  }
}
