package com.yzt.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark06_Req3_PageFlow {
  def main(args: Array[String]): Unit = {

    //TODO :页面跳转概率 (A->B)/A

    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)

    //TODO 1.读原始数据
    val actionRDD = sc.textFile("spark-core/user_visit_action.txt")
    actionRDD.cache()

    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong,
        )
      }
    )
    actionDataRDD.cache()
    //TODO 指定页面统计
    // 1-2,2-3,3-4,4-5,5-6,6-7
    val ids:List[Long]= List(1,2,3,4,5,6,7)
    val okIds:List[(Long,Long)] = ids.zip(ids.tail)

    //TODO 过滤
    val filterRDD = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    )
    filterRDD.cache()

    //TODO 计算分母
    val pageToCnt = filterRDD.map(
      action => {
        (action.page_id, 1)
      }
    ).reduceByKey(_ + _).collect().toMap

    //TODO 计算分子
    // 根据session分组，每组按时间排序
    val sessionRDD = actionDataRDD.groupBy(_.session_id)
    val pageflowCnt = sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        val flowIds = sortList.map(_.page_id)
        val pageflowIds = flowIds.zip(flowIds.tail)
        //将不合法的页面跳转过滤
        pageflowIds.filter(
          t=>{
            okIds.contains(t)
          }
        ).map(
          t=>{
            (t,1)
          }
        )
      }
    ).map(_._2).flatMap(List=>List).reduceByKey(_+_)

    //TODO 计算单跳转换率
    pageflowCnt.foreach{
      case ((p1,p2),c)=>{
        val i = pageToCnt.getOrElse(p1, 0)
        println(s"页面${p1}跳转到${p2}的单挑转换率为："+(c.toDouble/i))
      }
    }

    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
    date: String, //用户点击行为的日期
    user_id: Long, //用户的 ID
    session_id: String, //Session 的 ID
    page_id: Long, //某个页面的 ID
    action_time: String, //动作的时间点
    search_keyword: String, //用户搜索的关键词
    click_category_id: Long, //某一个商品品类的 ID
    click_product_id: Long, //某一个商品的 ID
    order_category_ids: String, //一次订单中所有品类的 ID 集合
    order_product_ids: String, //一次订单中所有商品的 ID 集合
    pay_category_ids: String, //一次支付中所有品类的 ID 集合
    pay_product_ids: String, //一次支付中所有商品的 ID 集合
    city_id: Long
  ) //城市 id

}
