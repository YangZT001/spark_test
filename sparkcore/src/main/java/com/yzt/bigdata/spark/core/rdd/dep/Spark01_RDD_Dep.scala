package com.yzt.bigdata.spark.core.rdd.dep

import com.yzt.bigdata.spark.core.rdd.serial.Spark01_RDD_Serial.Search
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    val lines = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("*****************************")

    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("*****************************")

    val wordToOne = words.map(
      word => (word, 1)
    )
    println(wordToOne.toDebugString)
    println("*****************************")

    val wordToCount = wordToOne.reduceByKey(_ + _)
    println(wordToCount.toDebugString)
    println("*****************************")

    val array = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
