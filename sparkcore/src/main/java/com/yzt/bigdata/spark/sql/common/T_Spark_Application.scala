package com.yzt.bigdata.spark.sql.common

import com.yzt.bigdata.spark.sql.util.EnvUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait T_Spark_Application {
  def start(master:String="local[*]",name:String="SQL")(op : => Unit):Unit={
    val conf = new SparkConf().setMaster(master).setAppName(name)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    EnvUtil.put(spark)

    try{
      op
    }catch {
      case ex => println(ex.getMessage)
    }


    spark.close()
    EnvUtil.clear()
  }
}
