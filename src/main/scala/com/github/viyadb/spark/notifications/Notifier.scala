package com.github.viyadb.spark.notifications

import com.github.viyadb.spark.Configs.{JobConf, NotifierConf}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

abstract class Notifier[A <: AnyRef](implicit m: Manifest[A]) extends Serializable {

  /**
    * Sends batch information over to the defined message queue type
    *
    * @param batchId Batch idenfitier
    * @param info    Batch info
    */
  def send(batchId: Long, info: A)

  /**
    * Retrieves latest notification that exists on the notification channel
    */
  def lastMessage: Option[A]

  /**
    * Retrieves all the notification that exist on the notification channel
    */
  def allMessages: Seq[A]

  protected def writeMessage(message: A): String = {
    implicit val formats = DefaultFormats
    write(message)
  }

  protected def readMessage(message: String): A = {
    implicit val formats = DefaultFormats
    read(message)
  }
}

object Notifier {
  def create[A <: AnyRef](jobConf: JobConf, notifierConf: NotifierConf)(implicit m: Manifest[A]): Notifier[A] = {
    notifierConf.`type` match {
      case "kafka" => new KafkaNotifier[A](jobConf, notifierConf)
      case "file" => new FileNotifier[A](jobConf, notifierConf)
      case other => throw new IllegalArgumentException(s"Unsupported notifier type: $other")
    }
  }
}
