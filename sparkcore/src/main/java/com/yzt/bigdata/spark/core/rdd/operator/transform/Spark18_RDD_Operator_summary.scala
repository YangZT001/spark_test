package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_summary {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 combineByKey
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)
    ), 2)
    //wordCount
    rdd.reduceByKey(_+_).collect().foreach(println)
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    rdd.foldByKey(0)(_+_).collect().foreach(println)
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y).collect().foreach(println)

    sc.stop()
  }
}
