package com.yzt.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_groupBy_Test {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 groupBy
    val rdd = sc.textFile("datas/apache.log")
    val timeRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour = sdf1.format(date)
        (hour, 1)
      }
    ).groupBy(_._1)
    timeRDD.map{
      case (hour,iter)=>{
        (hour,iter.size)
      }
    }.collect().foreach(println)
    //    val groupRDD = rdd.groupBy(_.split(" ")(3).split(":")(1))


    sc.stop()
  }
}
