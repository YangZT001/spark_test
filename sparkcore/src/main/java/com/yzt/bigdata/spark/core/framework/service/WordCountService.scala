package com.yzt.bigdata.spark.core.framework.service

import com.yzt.bigdata.spark.core.framework.common.TService
import com.yzt.bigdata.spark.core.framework.dao.WordCountDao

/**
 * 服务层
 */
class WordCountService extends TService{
   private val wordCountDao = new WordCountDao()

  def DataAnalysis(): Array[(String, Int)] ={
    val lines = wordCountDao.ReadFile("datas/1.txt")
    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )

    val wordToCount = wordToOne.reduceByKey(_ + _)

    wordToCount.collect()
  }
}
