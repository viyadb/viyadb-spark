package com.github.viyadb.spark.util

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.charset.StandardCharsets

import scala.collection.mutable.ListBuffer

class DummyStatsDServer(port: Int) {
  val messages: ListBuffer[String] = new ListBuffer[String]()
  private val server = DatagramChannel.open

  listen()

  private def listen() {
    server.bind(new InetSocketAddress(port))
    val thread = new Thread {
      override def run(): Unit = {
        val packet = ByteBuffer.allocate(1500)
        while (server.isOpen) {
          packet.clear()
          server.receive(packet)
          packet.flip()
          messages.appendAll(
            StandardCharsets.UTF_8.decode(packet).toString.split("\n").map(_.trim())
          )
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
  }

  def waitForMessage(): Unit = {
    while (messages.isEmpty) {
      Thread.sleep(50L)
    }
  }

  def close(): Unit = {
    server.close()
  }

  def clear(): Unit = {
    messages.clear()
  }
}
