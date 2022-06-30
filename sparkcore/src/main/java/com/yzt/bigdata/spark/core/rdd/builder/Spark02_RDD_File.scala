package com.yzt.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 从文件中创建RDD
    //path默认当前环境的根路径为基准，可以绝对路径也可以相对路径
    //val rdd = sc.textFile("datas/1.txt")
    //val rdd = sc.textFile("datas")
    val rdd = sc.textFile("datas/1*.txt")
    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }
}
