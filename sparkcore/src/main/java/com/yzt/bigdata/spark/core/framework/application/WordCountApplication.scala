package com.yzt.bigdata.spark.core.framework.application

import com.yzt.bigdata.spark.core.framework.common.TApplication
import com.yzt.bigdata.spark.core.framework.controller.WordCountController


object WordCountApplication extends App with TApplication{

  start(){
    val controller = new WordCountController
    controller.execute()
  }
}

