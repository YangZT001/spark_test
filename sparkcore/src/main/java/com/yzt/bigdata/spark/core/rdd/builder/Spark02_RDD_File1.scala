package com.yzt.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 从文件中创建RDD
    //textFile：以行为单位来读取数据，读取的都是字符串
    //wholeTextFiles：以文件为单位读取
    //  读取结果表示为元组，第一个文件路径，第二个文件内容
    val rdd = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }
}
