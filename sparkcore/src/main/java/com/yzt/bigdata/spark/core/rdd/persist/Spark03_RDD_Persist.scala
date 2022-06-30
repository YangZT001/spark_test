package com.yzt.bigdata.spark.core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("hello scala", "hello spark")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map((_, 1))
    //默认持久化操作，只能将数据保存到内存中，如果想到保存到磁盘文件中
    //需要自行选择级别
    //持久化操作必须在行动算子执行时完成的
//    mapRDD.persist(StorageLevel.DISK_ONLY)
    mapRDD.cache()
    println(mapRDD.toDebugString)
    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println("*************************************")
    println(mapRDD.toDebugString)

    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
