package com.yzt.bigdata.spark.core.req

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graalvm.compiler.code.DataSection.Data

import scala.util.Properties


object Spark03_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {

    //TODO :热门品类top10

    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)

    //Q : reduce 存在大量shuffle

    //TODO 1.读原始数据
    val actionRDD = sc.textFile("spark-core/user_visit_action.txt")
    actionRDD.cache()
    //TODO 2.统计点击数量（id，（1，0，0））
    //       统计下单数量（id，（0，1，0））
    //       统计支付数量（id，（0，0，1））

    //TODO 3.聚合（id，（点击数量，下单数量，支付数量））
    val flatRDD:RDD[(String,(Int,Int,Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          //点击
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          //下单
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          //支付
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    //TODO 4.将品类进行排序，并且取top10
    //  点击->下单->支付
    //  元组排序：先比较第一个，再比较第二个，以此类推
    //  (id,(点击数量, 下单数量, 支付数量))
    //  将三个数据源合并进行聚合计算
    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val result = analysisRDD.sortBy(_._2,false).take(10)

    //TODO 5.结果采集打印
    result.foreach(println)

    sc.stop()
  }
}
