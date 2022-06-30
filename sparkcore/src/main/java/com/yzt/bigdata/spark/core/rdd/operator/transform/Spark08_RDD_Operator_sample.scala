package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_sample {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 sample
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //三个参数
    //1.抽取数据后是否放回，true放回
    //2.  抽取不放回：每条数据可能被抽取的概率
    //    抽取放回：表示数据源中每条数据被抽取的可能次数
    //3.抽取数据时随机算法的种子
    val str = rdd.sample(true, 2).collect().mkString(",")
    println(str)
    sc.stop()
  }
}
