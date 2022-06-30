package com.yzt.bigdata.spark.core.rdd.IO

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_load {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val textRDD = sc.textFile("output1")
    println(textRDD.collect().mkString(","))

    val objectRDD = sc.objectFile[(String,Int)]("output2")
    println(objectRDD.collect().mkString(","))

    val seqRDD = sc.sequenceFile[String,Int]("output3")
    println(seqRDD.collect().mkString(","))

    sc.stop()
  }
}
