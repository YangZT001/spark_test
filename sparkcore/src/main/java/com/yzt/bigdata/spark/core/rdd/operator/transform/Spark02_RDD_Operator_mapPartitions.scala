package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_mapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子mapPartitions
    //以分区为单位进行转换操作，将整个分区的数据加载到内存中进行引用
    //如果处理完的数据是不会被释放的，存在对象的引用
    //在内存较小，数据量较大的情况会造成内存溢出
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    //    val mapRDD = rdd.mapPartitions(_.map(_ * 2))
    val mapRDD = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>")
        iter.map(_ * 2)
      }
    )
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
