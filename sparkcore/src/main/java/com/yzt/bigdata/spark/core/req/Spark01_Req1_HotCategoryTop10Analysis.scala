package com.yzt.bigdata.spark.core.req

import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Properties


object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {

    //TODO :热门品类top10

    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)


    //TODO 1.去读原始数据
    val actionRDD = sc.textFile("spark-core/user_visit_action.txt")

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
    ).reduceByKey(_ + _)

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
    ).reduceByKey(_ + _)
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
    ).reduceByKey(_ + _)

    //TODO 5.将品类进行排序，并且取top10
    //  点击->下单->支付
    //  元组排序：先比较第一个，再比较第二个，以此类推
    //  (id,(点击数量, 下单数量, 支付数量))
    val cogroupRDD = clickCountRDD.cogroup(
      orderCountRDD, payCountRDD
    )
    val analysisRDD = cogroupRDD.mapValues {
      case (c1, c2, c3) => {
        var clickCnt = 0
        if (c1.iterator.hasNext) {
          clickCnt = c1.iterator.next()
        }
        var orderCnt = 0
        if (c2.iterator.hasNext) {
          orderCnt = c2.iterator.next()
        }
        var payCnt = 0
        if (c3.iterator.hasNext) {
          payCnt = c3.iterator.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }
    val result = analysisRDD.sortBy(_._2,false).take(10)

    //TODO 6.结果采集打印
    result.foreach(println)


    sc.stop()
  }
}
