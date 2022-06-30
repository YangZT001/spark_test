package com.yzt.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_wordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      "hello", "spark", "hello", "scala"
    ))

    val scAcc = new MyAcc()
    sc.register(scAcc, "wordCountAcc")

    rdd.foreach(
      w => {
        scAcc.add(w)
      }
    )
    println(scAcc.value)
    sc.stop()
  }

  //自定义数据累加器
  /*
  1.集成AccumulatorV2，定义泛型
  IN：输入
  OUT：输出
  2.重写方法
   */
  class MyAcc extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private var wcMap = mutable.Map[String, Long]()
    //判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAcc()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取输入
    override def add(v: String): Unit = {
      val newCnt = wcMap.getOrElse(v, 0L) + 1
      wcMap.update(v,newCnt)
    }
    //合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach{
        case (w,c)=>{
          val newCnt = map1.getOrElse(w,0L)+c
          map1.update(w,newCnt)
        }
      }
    }
    //累加器结果
    override def value: mutable.Map[String,Long] = {
      wcMap
    }
  }

}
