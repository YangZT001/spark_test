package com.yzt.bigdata.spark.core.req

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Properties


object Spark02_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {

    //TODO :热门品类top10

    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)

    //Q : RDD重复使用
    //Q : cogroup性能可能比较低，有shuffle


    //TODO 1.去读原始数据
    val actionRDD = sc.textFile("spark-core/user_visit_action.txt")
    actionRDD.cache()
    //TODO 2.统计点击数量（id，点击数量）
    val clickRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )
    val clickCountRDD = clickRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _).map{
      case(s,c)=>{
        (s,(c,0,0))
      }
    }

    //TODO 3.统计下单数量（id，下单数量）
    val orderRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )
    val orderCountRDD = orderRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _).map{
      case(s,c)=>{
        (s,(0,c,0))
      }
    }
    //TODO 4.统计支付数量（id，支付数量）
    val payRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )
    val payCountRDD = payRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _).map{
      case(s,c)=>{
        (s,(0,0,c))
      }
    }

    //TODO 5.将品类进行排序，并且取top10
    //  点击->下单->支付
    //  元组排序：先比较第一个，再比较第二个，以此类推
    //  (id,(点击数量, 下单数量, 支付数量))
    //  将三个数据源合并进行聚合计算
    val analysisRDD = clickCountRDD.union(orderCountRDD).union(payCountRDD).reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val result = analysisRDD.sortBy(_._2,false).take(10)

    //TODO 6.结果采集打印
    result.foreach(println)


    sc.stop()
  }
}
