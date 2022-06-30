package com.yzt.bigdata.spark.core.test

class SubTask extends Serializable {
  var datas : List[Int]= _

  var logic:(Int)=>Int = _

  def compute(): Seq[Int] ={
    datas.map(logic)
  }


}
