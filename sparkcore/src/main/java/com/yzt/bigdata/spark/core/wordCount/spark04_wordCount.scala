package com.yzt.bigdata.spark.core.wordCount

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object spark04_wordCount {

  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", "D:\\hadoop-3.1.3")


    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    wordcount7(sc)
    //TODO 关闭连接
    sc.stop()
  }
  //groupby
  def wordcount1(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" "))
    val group = words.groupBy(w => w)
    val wordcount = group.mapValues(_.size)
    wordcount.collect().foreach(println)
  }
  //groupbyKey
  def wordcount2(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" ")).map((_,1))
    val group = words.groupByKey()
    val wordcount = group.mapValues(_.size)
    wordcount.collect().foreach(println)
  }
  //reduceByKey
  def wordcount3(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" ")).map((_,1))
    val wordcount = words.reduceByKey(_+_)
    wordcount.collect().foreach(println)
  }
  //aggregateByKey
  def wordcount4(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" ")).map((_,1))
    val wordcount = words.aggregateByKey(0)(_+_,_+_)
    wordcount.collect().foreach(println)
  }
  //foldByKey
  def wordcount5(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" ")).map((_,1))
    val wordcount = words.foldByKey(0)(_+_)
    wordcount.collect().foreach(println)
  }
  //combineByKey
  def wordcount6(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" ")).map((_,1))
    val wordcount = words.combineByKey(
      v=>v,
      (x:Int,y)=>x+y,
      (x:Int,y:Int)=>x+y
    )
    wordcount.collect().foreach(println)
  }
  //countByKey
  def wordcount7(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" ")).map((_,1))
    val wordcount = words.countByKey()
    println(wordcount.mkString(","))
  }
  //countByValue
  def wordcount8(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" "))
    val wordcount = words.countByValue()
    println(wordcount.mkString(","))
  }
  //reduce
  def wordcount9(sc:SparkContext)={
    val rdd = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val words = rdd.flatMap(_.split(" "))
    val mapWord = words.map(
      word => {
        mutable.Map[String,Long]((word,1))
      }
    )
    val wordcount = mapWord.reduce(
      (m1,m2)=>{
        m2.foreach{
          case (word,count)=>{
            val newCount = m1.getOrElse(word,0L)+count
            m1.update(word,newCount)
          }
        }
        m1
      }
    )
    println(wordcount.mkString(","))
  }
}
