package com.yzt.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01_SparkSQL_Basic {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        //dataframe
        val df = spark.read.json("datas/user.txt")
        df.show()

        //dataframe => SQL
        df.createOrReplaceTempView("user")
        spark.sql("select age from user where name=\"zhangsan\"").show()

        //dataframe => DSL
        //设计转换操作时，需要引入规则
        import spark.implicits._
        df.select("age","name").show()
        df.select('age+1,'name).show()

        //dataSet
        //dataframe是特殊的dataset
        val list = List(1,2,3,4)
        val ds = list.toDS()
        ds.show()

        //rdd <=> dataframe
        val rdd = spark.sparkContext.makeRDD(List(
            (1,"zhangsan",20),(2,"lisi",30),(3,"wangwu",40)
        ))
        val df1 = rdd.toDF("id","name","age")
        df1.show()
        val rowRDD = df1.rdd

        //dataframe <=> dataset
        val ds1 = df1.as[User]
        ds1.show()
        val newdf = ds1.toDF()
        newdf.show()

        //rdd <=> dataset
        val ds2 = rdd.map {
            case (id, name, age) => {
                User(id, name, age)
            }
        }.toDS()
        ds2.show()
        val userRDD = ds2.rdd

        spark.close()

    }
    case class User(id:Int,name:String,age:Int)
}
