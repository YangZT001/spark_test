package com.yzt.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.{ServerSocket, Socket}

object Executor2 {
  def main(args: Array[String]): Unit = {
    //启动服务器，接收数据
    val server = new ServerSocket(8888)
    println("服务器[8888]启动，等待接收数据")
    //等待连接
    val client: Socket = server.accept()

    val in = client.getInputStream

    val objIn = new ObjectInputStream(in)

    val task = objIn.readObject().asInstanceOf[SubTask]
    val ints = task.compute()


    println("[8888]计算结果",ints)

    objIn.close()
    client.close()
    server.close()
  }
}
