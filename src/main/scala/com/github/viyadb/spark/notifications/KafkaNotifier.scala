package com.github.viyadb.spark.notifications

import java.util.Properties

import com.github.viyadb.spark.Configs.NotifierConf
import com.github.viyadb.spark.util.KafkaUtil
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

class KafkaNotifier[A <: AnyRef](notifierConf: NotifierConf)(implicit m: Manifest[A]) extends Notifier[A] {

  @transient
  private lazy val producer = createProducer()

  private def createProducer() = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, notifierConf.channel)
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
    new KafkaProducer[String, String](props)
  }

  override def send(batchId: Long, info: A) = {
    producer.send(
      new ProducerRecord[String, String](notifierConf.queue, batchId.toString, writeMessage(info))).get
  }

  override def lastMessage = {
    val latestOffsets = KafkaUtil.latestOffsets(notifierConf.channel, Set(notifierConf.queue))
    if (latestOffsets.isEmpty) {
      None
    } else {
      val lastElementRdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
        SparkSession.builder().getOrCreate().sparkContext,
        Map("metadata.broker.list" -> notifierConf.channel),
        latestOffsets.map(latestOffset => OffsetRange(
          latestOffset._1,
          if (latestOffset._2 > 0) latestOffset._2 - 1 else latestOffset._2,
          latestOffset._2)).toArray
      )
      lastElementRdd.collect().map { case (id, value) => (id, readMessage(value)) }.
        sortBy(_._1).map(_._2).lastOption
    }
  }

  override def allMessages = {
    val earliestOffsets = KafkaUtil.earliestOffsets(notifierConf.channel, Set(notifierConf.queue))
    val latestOffsets = KafkaUtil.latestOffsets(notifierConf.channel, Set(notifierConf.queue))

    val offsetRanges = earliestOffsets.map { case (topicPartition, fromOffsets) =>
      val toOffsets = latestOffsets(topicPartition)
      OffsetRange(topicPartition.topic, topicPartition.partition, fromOffsets, toOffsets)
    }.toArray

    if (offsetRanges.isEmpty || latestOffsets.isEmpty) {
      Seq()
    } else {
      val lastElementRdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
        SparkSession.builder().getOrCreate().sparkContext,
        Map("metadata.broker.list" -> notifierConf.channel),
        offsetRanges
      )
      lastElementRdd.collect().map { case (_, value) => readMessage(value) }
    }
  }
}
