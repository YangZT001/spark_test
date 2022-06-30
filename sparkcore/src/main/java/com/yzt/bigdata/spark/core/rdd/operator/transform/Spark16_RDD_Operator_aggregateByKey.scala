package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 aggregateByKey
    //分区里面的元素进行聚合
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("a",4)
    ),2)
    //("a",[1,2]),("a",[3,4])
    //("a",2),("a",4)
    //("a",6)
    //有两个参数列表
    //1.初始值，主要用于第一个key和value进行分区计算
    //2.需要两个参数
    //  1.分区内计算规则
    //  2.分区间计算规则
    val groupRDD = rdd.aggregateByKey(0)(
      (x,y)=>math.max(x,y),
      (x,y)=>x+y
    )
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
