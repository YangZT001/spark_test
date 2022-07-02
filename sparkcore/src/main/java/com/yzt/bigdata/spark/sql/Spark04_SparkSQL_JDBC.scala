package com.yzt.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object Spark04_SparkSQL_JDBC {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._
      //读取mysql数据
      val df = spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/user")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "111111")
        .option("dbtable", "user")
        .load()
      df.show()

        spark.close()
    }

}
