package com.yzt.bigdata.spark.core.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    var rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxxxxxx"),
      ("cba", "xxxxxxxxxxxx"),
      ("wnba", "xxxxxxxxxxxx"),
      ("nba", "xxxxxxxxxxxx")
    ))
    val partRDD = rdd.partitionBy(new MyPartition)
    partRDD.saveAsTextFile("output")
    sc.stop()

  }
  //自定义分区
  class MyPartition extends Partitioner{
    //分区数量
    override def numPartitions: Int = 3
    //返回数据的key的分区索引，从0开始
    override def getPartition(key: Any): Int = {
      key match {
        case "nba"=>0
        case "wnba"=>1
        case _ =>2
      }
    }
  }
}
