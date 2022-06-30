package com.yzt.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    val lines = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("*****************************")

    val words = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("*****************************")

    val wordToOne = words.map(
      word => (word, 1)
    )
    println(wordToOne.dependencies)
    println("*****************************")

    val wordToCount = wordToOne.reduceByKey(_ + _)
    println(wordToCount.dependencies)
    println("*****************************")

    val array = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
