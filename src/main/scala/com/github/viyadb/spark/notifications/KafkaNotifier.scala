package com.github.viyadb.spark.notifications

import java.util.Properties

import com.github.viyadb.spark.Configs.{JobConf, NotifierConf}
import com.github.viyadb.spark.util.KafkaOffsetUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._

class KafkaNotifier[A <: AnyRef](jobConf: JobConf, notifierConf: NotifierConf)(implicit m: Manifest[A])
  extends Notifier[A] with Logging {

  @transient
  private lazy val producer: KafkaProducer[String, String] = createProducer()

  private val consumerConf = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> notifierConf.channel,
    ConsumerConfig.GROUP_ID_CONFIG -> jobConf.indexer.id,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )

  private def createProducer(): KafkaProducer[String, String] = {
    val producerConf = new Properties()
    producerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, notifierConf.channel)
    producerConf.put(ProducerConfig.RETRIES_CONFIG, "3")
    producerConf.put(ProducerConfig.ACKS_CONFIG, "all")
    producerConf.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Integer.MAX_VALUE.toString)
    producerConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    producerConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    new KafkaProducer[String, String](producerConf)
  }

  override def send(batchId: Long, info: A): Unit = {
    val message = writeMessage(info)
    logInfo(s"Sending message: $message")
    producer.send(
      new ProducerRecord[String, String](notifierConf.queue, batchId.toString, message)).get
  }

  override def lastMessage: Option[A] = {
    val latestOffsets = KafkaOffsetUtils.latestOffsets(notifierConf.channel, Set(notifierConf.queue))
    if (latestOffsets.isEmpty) {
      None
    } else {
      val offsetRanges = latestOffsets.map(latestOffset => OffsetRange(
        latestOffset._1,
        if (latestOffset._2 > 0) latestOffset._2 - 1 else latestOffset._2,
        latestOffset._2)).toArray

      val lastElementRdd = KafkaUtils.createRDD[String, String](
        SparkSession.builder().getOrCreate().sparkContext,
        consumerConf.asJava,
        offsetRanges,
        LocationStrategies.PreferConsistent
      )

      val lastMessage = lastElementRdd.map { r => (r.key(), readMessage(r.value())) }
        .collect().sortBy(_._1).map(_._2).lastOption

      if (lastMessage.nonEmpty) {
        logInfo(s"Read last message: $lastMessage")
      }
      lastMessage
    }
  }

  override def allMessages: Seq[A] = {
    val earliestOffsets = KafkaOffsetUtils.earliestOffsets(notifierConf.channel, Set(notifierConf.queue))
    val latestOffsets = KafkaOffsetUtils.latestOffsets(notifierConf.channel, Set(notifierConf.queue))

    val offsetRanges = earliestOffsets.map { case (topicPartition, fromOffsets) =>
      val toOffsets = latestOffsets(topicPartition)
      OffsetRange(topicPartition.topic, topicPartition.partition, fromOffsets, toOffsets)
    }.toArray

    if (offsetRanges.isEmpty || latestOffsets.isEmpty) {
      Seq()
    } else {
      val lastElementRdd = KafkaUtils.createRDD[String, String](
        SparkSession.builder().getOrCreate().sparkContext,
        consumerConf.asJava,
        offsetRanges,
        LocationStrategies.PreferConsistent
      )
      lastElementRdd.map { record => readMessage(record.value()) }.collect()
    }
  }
}
