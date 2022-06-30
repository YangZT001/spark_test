package com.yzt.bigdata.spark.core.test


//extends Serializable 序列化发送
class Task extends Serializable {
  val datas = List(1,2,3,4)

//  val logic = ( num:Int )=>{ num * 2 }
  val logic : (Int)=>Int = _*2

}
