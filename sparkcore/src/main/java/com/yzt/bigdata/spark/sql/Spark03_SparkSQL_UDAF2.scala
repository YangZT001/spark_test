package com.yzt.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object Spark03_SparkSQL_UDAF2 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        val df = spark.read.json("datas/user.txt")

        //早期版本中，spark不能在sql中使用强类型UDAF操作
        //早期的强类型聚合函数使用DSL语法操作
        import spark.implicits._
        val ds = df.as[User]

        //将udaf转换成查询的列对象
        val udafClo = new MyAvg().toColumn
        ds.select(udafClo).show()

        spark.close()
    }
    case class User(var name:String,age:Long)
    case class Buff(var total:Long,var count:Long)
  /***
     * 自定义聚合函数类，计算平均值
     * 1.继承Aggregator，定义泛型
     * IN : 输入类型
     * BUF : 缓冲区数据类型
   * OUT : 输出类型
     * 2.重写方法
     */
    class MyAvg extends Aggregator[User,Buff,Long]{
      //初始值或零值
      override def zero: Buff = {
          new Buff(0L,0L)
      }
      //根据输入更新缓冲区
      override def reduce(b: Buff, a: User): Buff = {
          b.count+=1
          b.total+=a.age
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
