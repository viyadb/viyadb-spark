package com.github.viyadb.spark.streaming.notifier

import com.github.viyadb.spark.Configs.NotifierConf

abstract class MicroBatchNotifier(config: NotifierConf) extends Serializable {

  /**
    * Sends micro-batch information over to the defined message queue type
    *
    * @param info Micro-batch info
    */
  def notify(info: MicroBatchInfo)
}

object MicroBatchNotifier {
  def create(config: NotifierConf): MicroBatchNotifier = {
    config.`type` match {
      case "kafka" => new KafkaMicroBatchNotifier(config)
      case _ => throw new IllegalArgumentException(s"Unsupported notifier type: ${config.`type`}")
    }
  }
}
