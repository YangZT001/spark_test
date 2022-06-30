package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_reduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 reduceByKey
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("b",4)
    ))
    //相同的key进行value的聚合
    //key只有一个不会参与计算
    //支持分区内预聚合功能，减少shuffle时落盘的数据量
    val reduceRDD = rdd.reduceByKey((x:Int,y:Int)=>{
      println(s"x=${x},y=${y}")
      x+y
    })
    reduceRDD.collect().foreach(print)
    sc.stop()
  }
}
