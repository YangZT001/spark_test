package com.yzt.bigdata.spark.core.wordCount

import org.apache.spark.{SparkConf, SparkContext}

object spark03_wordCount {

  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", "D:\\hadoop-3.1.3")


    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    val lines = sc.textFile("datas")

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )
//    spark框架将分组和聚合使用一个方法实现
//    reduceByKey 相同的key进行reduce聚合
//    wordToOne.reduceByKey((x,y)=>x+y)
    val wordToCount = wordToOne.reduceByKey(_ + _)

    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop()
  }

}
