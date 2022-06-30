package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_combineByKey {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 combineByKey
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    //获取相同key的平均值(a,3)(b,4)
    //combineByKey 三个参数
    //1.相同key的第一个数据进行结构转化
    //2.分区内的计算规则
    //3.分区间的计算规则
    val aggRDD = rdd.combineByKey(
      v=>(v,1),
      (t:(Int,Int),v)=>{
      (t._1+v,t._2+1)
      },
      (t1:(Int,Int),t2:(Int,Int))=>{
        (t1._1+t2._1,t1._2+t2._2)
      }
    )

    aggRDD.map{
      case (k,t)=>{
        (k,t._1/t._2)
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
