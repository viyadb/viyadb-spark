package com.github.viyadb.spark.util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._

object KafkaOffsetUtils extends Logging {
  def fetchOffsets(brokers: String, topics: Set[String], earleiest: Boolean): Map[TopicPartition, Long] = {
    logInfo(s"Fetching ${if (earleiest) "earliest" else "latest"} offsets from Kafka")

    val consumerConf = new Properties()
    consumerConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    consumerConf.put(ConsumerConfig.GROUP_ID_CONFIG, "viyadb-offset-utils")
    consumerConf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerConf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerConf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)
    val consumer = new KafkaConsumer[String, String](consumerConf)

    try {
      val topicsPartitions = for {
        topic <- topics
        partitionInfo <- consumer.partitionsFor(topic)
      } yield {
        new TopicPartition(partitionInfo.topic(), partitionInfo.partition())
      }

      val offsets = if (earleiest) consumer.beginningOffsets(topicsPartitions) else
        consumer.endOffsets(topicsPartitions)

      offsets.map(x => (new TopicPartition(x._1.topic(), x._1.partition()), x._2.longValue())).toMap

    } finally {
      consumer.close()
    }
  }

  def latestOffsets(brokers: String, topics: Set[String]): Map[TopicPartition, Long] = fetchOffsets(brokers, topics, earleiest = false)

  def earliestOffsets(brokers: String, topics: Set[String]): Map[TopicPartition, Long] = fetchOffsets(brokers, topics, earleiest = true)
}
