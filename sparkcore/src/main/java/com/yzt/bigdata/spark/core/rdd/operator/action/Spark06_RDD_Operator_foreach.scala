package com.yzt.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_foreach {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkconf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    val user1 = new User1()
    val user2 = new User2()
    //rdd算子中传递的函数是会包含闭包操作，那么会进行检测功能
    rdd.foreach({
      num =>{
        println("User1 age"+ num +" = "+(user1.age+num))
        println("User2 age"+ num +" = "+(user2.age+num))
      }
    })

    sc.stop()
  }
  //方法1
  //没有Serializable报错
  class User1 extends Serializable{
    var age :Int = 30
  }
  //方法2
  //样例类在编译时，会自动混入序列化特质
  case class User2() {
    var age :Int = 30
  }


}
