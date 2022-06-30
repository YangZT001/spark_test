package com.yzt.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object Spark03_SparkSQL_UDAF1 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        val df = spark.read.json("datas/user.txt")
        df.createOrReplaceTempView("user")

        spark.udf.register("myAvg",functions.udaf(new MyAvg))

        spark.sql("select myAvg(age) from user").show()

        spark.close()

    }
    case class Buff(var total:Long,var count:Long)
  /***
   * 强类型聚合函数
     * 自定义聚合函数类，计算平均值
     * 1.继承Aggregator，定义泛型
   * IN : 输入类型
   * BUF : 缓冲区数据类型
   * OUT : 输出类型
     * 2.重写方法
     */
    class MyAvg extends Aggregator[Long,Buff,Long]{
      //初始值或零值
      override def zero: Buff = {
          new Buff(0L,0L)
      }
      //根据输入更新缓冲区
      override def reduce(b: Buff, a: Long): Buff = {
          b.count+=1
          b.total+=a
          b
      }
      //合并缓冲区
      override def merge(b1: Buff, b2: Buff): Buff = {
          b1.count+=b2.count
          b1.total+=b2.total
          b1
      }
      //计算结果
      override def finish(reduction: Buff): Long = {
          reduction.total/reduction.count
      }
      //缓冲区编码操作
      override def bufferEncoder: Encoder[Buff] = Encoders.product

      //输出编码操作
      override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
