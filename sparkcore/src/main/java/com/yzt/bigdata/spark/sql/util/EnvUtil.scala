package com.yzt.bigdata.spark.sql.util

import org.apache.spark.sql.SparkSession

object EnvUtil {
  private val spark = new ThreadLocal[SparkSession]()

  def put(ss :SparkSession):Unit={
    spark.set(ss)
  }
  def take():SparkSession={
    spark.get()
  }
  def clear(): Unit ={
    spark.remove()
  }
}
