package com.yzt.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark02_SparkSQL_UDF {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        val df = spark.read.json("datas/user.txt")
        df.createOrReplaceTempView("user")

        spark.udf.register("prefixName",(name:String)=>{
            "Name:"+name
        })

        spark.sql("select age , prefixName(name) from user").show()

        spark.close()

    }
}
