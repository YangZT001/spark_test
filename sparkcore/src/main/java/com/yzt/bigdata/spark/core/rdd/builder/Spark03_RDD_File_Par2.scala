package com.yzt.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 从文件中创建RDD
    //textFile也可以分区,默认2
    //文件中回车也是大小
    val rdd = sc.textFile("datas/word.txt",2)

    rdd.saveAsTextFile("output")


    //TODO 关闭环境
    sc.stop()
  }
}
