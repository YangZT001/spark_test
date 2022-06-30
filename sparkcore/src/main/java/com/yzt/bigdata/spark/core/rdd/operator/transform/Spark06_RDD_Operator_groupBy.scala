package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_groupBy {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 groupBy
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    //将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    //将相同的放在一个组中
    def groupFunction(num:Int):Int={
      num%2
    }

    val groupRDD = rdd.groupBy(groupFunction)

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
