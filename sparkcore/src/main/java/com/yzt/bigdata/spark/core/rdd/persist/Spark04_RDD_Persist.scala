package com.yzt.bigdata.spark.core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    sc.setCheckpointDir("cp")

    val list = List("hello scala", "hello spark")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map((_, 1))
    //需要落盘，必须指定检查点保存路径
    //一般保存路径都是在分布式存储系统：HDFS中
    mapRDD.checkpoint()
    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println("*************************************")

    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
