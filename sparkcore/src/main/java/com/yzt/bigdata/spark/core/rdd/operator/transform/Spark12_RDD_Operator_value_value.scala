package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_value_value {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 双value
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))
    val rdd7 = sc.makeRDD(List("a","b","c","d"))

    //交集[3,4],需同类型
    val rdd3 = rdd1.intersection(rdd2)
    println("交集:",rdd3.collect().mkString(","))
    //并集[1,2,3,4,3,4,5,6],需同类型
    val rdd4 = rdd1.union(rdd2)
    println("并集:",rdd4.collect().mkString(","))
    //差集,rdd1-rdd2[1,2],需同类型
    val rdd5 = rdd1.subtract(rdd2)
    println("差集:",rdd5.collect().mkString(","))
    //拉链[1-3,2-4,3-5,4,6],不需同类型
    val rdd6 = rdd1.zip(rdd2)
    println("拉链(T,T):",rdd6.collect().mkString(","))
    val rdd8 = rdd1.zip(rdd7)
    println("拉链(T,U):",rdd8.collect().mkString(","))

    sc.stop()
  }
}
