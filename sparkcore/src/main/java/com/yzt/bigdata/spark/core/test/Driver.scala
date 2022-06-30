package com.yzt.bigdata.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
  //连接服务器
    val client1 = new Socket("localhost",9999)
    val client2 = new Socket("localhost",8888)

    val out1= client1.getOutputStream
    val objOut1 = new ObjectOutputStream(out1)

    val task = new Task()
    val subTask1 = new SubTask()
    subTask1.logic = task.logic
    subTask1.datas = task.datas.take(2)

    objOut1.writeObject(subTask1)
    objOut1.flush()
    objOut1.close()
    client1.close()
    println("数据发送完毕")
//------------------------------------------------------
    val out2= client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2)

    subTask1.datas = task.datas.takeRight(2)
    objOut2.writeObject(subTask1)
    objOut2.flush()
    objOut2.close()
    client2.close()
    println("数据发送完毕")
  }
}
