package com.yzt.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark05_Req2_HotCategoryTop10Session {
  def main(args: Array[String]): Unit = {

    //TODO :热门品类top10中每个品类的Top10的活跃Session统计

    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)

    //Q : reduce 存在大量shuffle

    //TODO 1.读原始数据
    val actionRDD = sc.textFile("spark-core/user_visit_action.txt")
    actionRDD.cache()

    val top10Ids = top10Catogory(actionRDD)
    //TODO 2.过滤原始数据，保留点击和前10品类的ID
    val filterRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        if (datas != "-1") {
          top10Ids.contains(datas(6))
        } else {
          false
        }
      }
    )
    //TODO 3.根据ID和SseeionID进行点击量统计
    val cntRDD = filterRDD.map(
      action => {
        val datas = action.split("_")
        ((datas(6), (datas(2))), 1)
      }
    ).reduceByKey(_ + _).map {
      case ((id, sid), cnt) => {
        (id, (sid, cnt))
      }
    }
    //TODO 4.相同品类ID进行分组
    val groupRDD = cntRDD.groupByKey()

    //TODO 5.排序取10
    val result = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    result.collect().foreach(println)

    sc.stop()
  }

  def top10Catogory(actionRDD: RDD[String]) = {
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    analysisRDD.sortBy(_._2, false).take(10).map(_._1)

  }
}
