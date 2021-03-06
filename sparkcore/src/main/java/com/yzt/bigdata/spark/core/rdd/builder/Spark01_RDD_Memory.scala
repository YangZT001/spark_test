package com.yzt.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 创建RDD
    //将内存中集合的数据作为数据源
    val seq = Seq[Int](1,2,3,4)
    //val rdd = sc.parallelize(seq)
    //makeRDD底层实际调用了parallelize
    val rdd = sc.makeRDD(seq)
    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
