package com.github.viyadb.spark.streaming.notifier

import java.util.Properties

import com.github.viyadb.spark.Configs.NotifierConf
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

class KafkaMicroBatchNotifier(config: NotifierConf) extends MicroBatchNotifier(config) {

  lazy private val producer = createProducer()

  private def createProducer() = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.host)
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
    new KafkaProducer[String, String](props)
  }

  override def notify(info: MicroBatchInfo) = {
    implicit val formats = DefaultFormats

    producer.send(
      new ProducerRecord[String, String](config.topic, info.time.toString(), write(info))).get
  }
}
