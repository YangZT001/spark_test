package com.yzt.bigdata.spark.core.framework.common

import com.yzt.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

trait TDao {
  def ReadFile(path: String): RDD[String] = {
    EnvUtil.take().textFile(path)
  }
}
