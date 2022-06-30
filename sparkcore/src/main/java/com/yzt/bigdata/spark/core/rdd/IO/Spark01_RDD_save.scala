package com.yzt.bigdata.spark.core.rdd.IO

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_save {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a",1),
      ("b",2),
      ("c",3),
    ))
    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")
    sc.stop()
  }
}
