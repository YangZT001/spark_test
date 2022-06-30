package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_test {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO
    //数据：时间戳，省份，城市，用户，广告
    //统计每个省份，用户点击前三的广告
    val datas = sc.textFile("datas/agent.log")

    val rdd = datas.map(
      lines => {
        val data = lines.split(" ")
        ((data(1),data(4)),1)
      }
    )
    val rdd1 = rdd.reduceByKey(_ + _).sortBy(_._2)
    rdd1.map{
      case(t,v)=>{
        (t._1,(t._2,v))
      }
    }.groupByKey().map{
      case(k,iter) =>{
        (k,iter.takeRight(3))
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
