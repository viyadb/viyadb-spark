package com.github.viyadb.spark.util

import kafka.common.TopicAndPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka.KafkaCluster

object KafkaUtil extends Logging {

  def latestOffsets(brokers: String, topics: Set[String]): Map[TopicAndPartition, Long] = {
    logInfo("Fetching latest offsets from Kafka")

    val kafkaParams = Map("metadata.broker.list" -> brokers)
    val kafkaCluster = new KafkaCluster(kafkaParams)
    val offsets = for {
      topicPartitions <- kafkaCluster.getPartitions(topics).right
      leaderOffsets <- kafkaCluster.getLatestLeaderOffsets(topicPartitions).right
    } yield {
      leaderOffsets.mapValues(_.offset)
    }
    offsets.right.getOrElse(Map())
  }

  def earliestOffsets(brokers: String, topics: Set[String]): Map[TopicAndPartition, Long] = {
    logInfo("Fetching earliest offsets from Kafka")

    val kafkaParams = Map("metadata.broker.list" -> brokers)
    val kafkaCluster = new KafkaCluster(kafkaParams)
    val offsets = for {
      topicPartitions <- kafkaCluster.getPartitions(topics).right
      leaderOffsets <- kafkaCluster.getEarliestLeaderOffsets(topicPartitions).right
    } yield {
      leaderOffsets.mapValues(_.offset)
    }
    offsets.right.getOrElse(Map())
  }
}
