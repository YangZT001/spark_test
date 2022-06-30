package com.yzt.bigdata.spark.core.wordCount

import org.apache.spark.{SparkConf, SparkContext}

object spark02_wordCount {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-3.1.3")


    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    val lines = sc.textFile("datas")

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )

    val wordGroup = wordToOne.groupBy(
      t => t._1
    )

    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1,t2) => {
            (t1._1,t1._2+t2._2)
          }
        )
      }
    }

    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop()
  }

}
