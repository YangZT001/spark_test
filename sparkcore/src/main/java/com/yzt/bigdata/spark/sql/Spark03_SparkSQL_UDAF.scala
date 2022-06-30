package com.yzt.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object Spark03_SparkSQL_UDAF {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        val df = spark.read.json("datas/user.txt")
        df.createOrReplaceTempView("user")

        spark.udf.register("myAvg",new MyAvg)

        spark.sql("select myAvg(age) from user").show()

        spark.close()

    }

    /***
     * 弱类型聚合函数
     * 自定义聚合函数类，计算平均值
     * 1.继承UserDefinedAggregateFunction
     * 2.重写方法
     */
    class MyAvg extends UserDefinedAggregateFunction{
        //输入数据的结构
        override def inputSchema: StructType = {
            StructType{
                Array(
                    StructField("age",LongType)
                )
            }
        }
        //缓冲区数据的结构
        override def bufferSchema: StructType = {
            StructType{
                Array(
                    StructField("total",LongType),
                    StructField("count",LongType)
                )
            }
        }
        //输出类型
        override def dataType: DataType = LongType
        //函数稳定性
        override def deterministic: Boolean = true
        //缓冲区初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0)=0L
            buffer(1)=0L
            //等同于
            //buffer.update(0,0L)
            //buffer.update(1,OL)

        }
        //根据输入更新缓冲区
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            //getLong获取当前值
            buffer.update(0,buffer.getLong(0)+input.getLong(0))
            buffer.update(1,buffer.getLong(1)+1)
        }
        //缓冲区数据合并
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
            buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
        }
        //计算
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0)/buffer.getLong(1)
        }
    }
}
