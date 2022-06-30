package com.yzt.bigdata.spark.core.wordCount

import org.apache.spark.{SparkConf, SparkContext}

object spark01_wordCount {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-3.1.3")
    //application
    //spark framework
    //TODO 建立连接spark框架
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    //1.读取文件，获取行数据
    val lines = sc.textFile("datas")

    //2.行数据拆分，形成单词（分词）
    //flatMap 扁平映射
    val words = lines.flatMap(_.split(" "))

    //3.讲数据根据分词进行分组，便于统计
    val wordGroup = words.groupBy(word => word)

    //4.对分组数据进行转换
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    //5.将转换结果采集到控制台打印
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop()
  }

}
