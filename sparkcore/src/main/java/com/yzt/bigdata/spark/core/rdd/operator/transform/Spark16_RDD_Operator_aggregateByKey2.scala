package com.yzt.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_aggregateByKey2 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkconf)

    //TODO 算子 aggregateByKey
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    //获取相同key的平均值(a,3)(b,4)
    val aggRDD = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    //    val newRDD = aggRDD.mapValues{
    //      case (num,cnt)=>{
    //        num/cnt
    //      }
    //    }
    val newRDD = aggRDD.map {
      case (k, t) => {
        (k,t._1/t._2)
      }
    }

    aggRDD.collect().foreach(println)
    newRDD.collect().foreach(println)
    sc.stop()
  }
}
