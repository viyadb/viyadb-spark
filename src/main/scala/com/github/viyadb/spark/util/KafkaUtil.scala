package com.github.viyadb.spark.util

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster

object KafkaUtil {

  def latestOffsets(brokers: String, topics: Set[String]): Map[TopicAndPartition, Long] = {
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
