package com.yzt.bigdata.spark.core.req

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Spark04_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {

    //TODO :热门品类top10

    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)


    //Q : 累加器替代reduce去掉shuffle

    val hotCategoryAcc = new HotCategoryAcc
    sc.register(hotCategoryAcc, "HotCategoryACC")


    //TODO 1.读原始数据
    val actionRDD = sc.textFile("spark-core/user_visit_action.txt")
    actionRDD.cache()


    //TODO 2.累加
    val analysisRDD = actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          //点击
          hotCategoryAcc.add((datas(6), "click"))
        } else if (datas(8) != "null") {
          //下单
          val ids = datas(8).split(",")
          ids.foreach(
            id => {
              hotCategoryAcc.add((id, "order"))
            }
          )
        } else if (datas(10) != "null") {
          //支付
          val ids = datas(10).split(",")
          ids.foreach(
            id => {
              hotCategoryAcc.add((id, "pay"))
            }
          )
        }
      }
    )

    val accMap = hotCategoryAcc.value.values.map(
      hc => {
        (hc.cid, (hc.clickCnt, hc.orderCnt, hc.payCnt))
      }
    ).toList
    val mapRDD = sc.makeRDD(accMap)
    val result = mapRDD.sortBy(_._2, ascending = false).take(10)

    //TODO 5.结果采集打印
    result.foreach(println)

    sc.stop()
  }

  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int) {

  }

  //自定义累加器
  //IN : （id，行为类型）
  //OUT ：mutable.Map[String,HotCategory]
  class HotCategoryAcc extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val result = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      result.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAcc()
    }

    override def reset(): Unit = {
      result.clear()
    }

    override def add(v: (String, String)): Unit = {
      val id = v._1
      val actionType = v._2
      val category = result.getOrElse(id, HotCategory(id, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      result.update(id, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      var r1 = this.result
      var r2 = other.value

      r2.foreach {
        case (id, hc) => {
          val category = r1.getOrElse(id, HotCategory(id, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          r1.update(id, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = result
  }

}
