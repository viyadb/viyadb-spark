package com.github.viyadb.spark.streaming.kafka

import com.github.viyadb.spark.TableConfig
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}

/**
  * Saves/Loads consumer offsets
  *
  * @param kafka Kafka source configuration
  */
abstract class OffsetStore(kafka: TableConfig.Kafka) {

  @transient
  lazy val log = org.apache.log4j.Logger.getLogger(getClass)

  case class Offset(topic: String, partition: Int, offset: Long) extends Serializable

  /**
    * Dirty method (because of reflection use) of fetching latest offsets from Kafka brokers.
    *
    * @return Offsets per topic and partition
    */
  protected def fetchLatest(): Map[TopicAndPartition, Long] = {
    log.info("Fetching latest offsets from Kafka")

    val kafkaParams = Map("metadata.broker.list" -> kafka.brokers.mkString(","))
    val kafkaCluster = new KafkaCluster(kafkaParams)

    val getFromOffsets = KafkaUtils.getClass.getDeclaredMethods.filter(_.getName == "getFromOffsets")(0)
    getFromOffsets.setAccessible(true)

    val kafkaUtilsInstance = KafkaUtils.getClass.getField("MODULE$").get(null)

    getFromOffsets.invoke(kafkaUtilsInstance, kafkaCluster.asInstanceOf[Object], kafkaParams, kafka.topics)
      .asInstanceOf[Map[TopicAndPartition, Long]]
  }

  /**
    * This method returns stored offsets or empty map if offsets were never stored.
    *
    * @return Stored offsets
    */
  protected def load(): Map[TopicAndPartition, Long]

  /**
    * This method loads latest saved offsets merged with fetched latest Kafka offsets.
    *
    * @return Latest available offsets
    */
  def loadOrFetch(): Map[TopicAndPartition, Long] = {
    val storedOffsets = load()
    val latestOffsets = fetchLatest()
    latestOffsets.map { case (k, v) => k -> storedOffsets.getOrElse(k, v) }
  }

  def save(offsets: Map[TopicAndPartition, Long])
}

object OffsetStore {
  def create(kafka: TableConfig.Kafka): OffsetStore = {
    if (kafka.offsetPath.nonEmpty) {
      new FileSystemOffsetStore(kafka)
    }
    throw new IllegalArgumentException("No Kafka offset storage is defined (offsetPath must be set)!")
  }
}
