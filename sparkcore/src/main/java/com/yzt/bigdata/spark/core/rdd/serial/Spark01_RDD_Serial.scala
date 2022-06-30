package com.yzt.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Serial")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(
      "hello world", "hello spark", "hive"
    ))
    val search = new Search("hello")
    val newRDD1 = search.getMatch1(rdd)
    val newRDD2 = search.getMatch2(rdd)
    newRDD1.collect().foreach(println)
    newRDD2.collect().foreach(println)

    sc.stop()
  }

//类的构造参数是类的属性，需要进行闭包检测，等同于类进行闭包检测
  class Search(q: String) extends Serializable {

    def isMatch(s: String): Boolean = {
      s.contains(q)
    }

    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(q))
    }
  }

}
