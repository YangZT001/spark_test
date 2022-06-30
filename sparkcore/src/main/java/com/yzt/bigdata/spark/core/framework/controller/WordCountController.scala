package com.yzt.bigdata.spark.core.framework.controller

import com.yzt.bigdata.spark.core.framework.common.TController
import com.yzt.bigdata.spark.core.framework.service.WordCountService

/**
 * 控制层
 */
class WordCountController extends TController{
  private val wordCountService = new WordCountService()
  def execute(): Unit ={
    var array = wordCountService.DataAnalysis()
    array.foreach(println)
  }
}
