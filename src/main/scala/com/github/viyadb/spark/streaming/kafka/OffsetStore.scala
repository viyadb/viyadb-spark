package com.github.viyadb.spark.streaming.kafka

import com.github.viyadb.spark.Configs.{JobConf, OffsetStoreConf}
import com.github.viyadb.spark.util.FileSystemUtil
import kafka.common.TopicAndPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

import scala.util.Try

/**
  * Saves/Loads consumer offsets
  *
  * @param config Kafka source configuration
  */
abstract class OffsetStore(config: JobConf) extends Logging with Serializable {

  case class Offset(topic: String, partition: Int, offset: Long) extends Serializable

  protected def serializeOffsets(offsets: Map[TopicAndPartition, Long]): String = {
    implicit val formats = DefaultFormats
    write(offsets.map(o => Offset(o._1.topic, o._1.partition, o._2)).toList)
  }

  protected def deserializeOffsets(json: String): Map[TopicAndPartition, Long] = {
    implicit val formats = DefaultFormats
    read[List[Offset]](json).map(o => (TopicAndPartition(o.topic, o.partition), o.offset)).toMap
  }

  /**
    * Dirty method (because of reflection use) of fetching latest offsets from Kafka brokers.
    *
    * @return Offsets per topic and partition
    */
  protected def fetchLatest(): Map[TopicAndPartition, Long] = {
    logInfo("Fetching latest offsets from Kafka")

    val kafkaParams = Map("metadata.broker.list" -> config.table.realTime.kafka.get.brokers.mkString(","))
    val kafkaCluster = new KafkaCluster(kafkaParams)

    val getFromOffsets = KafkaUtils.getClass.getDeclaredMethods.filter(_.getName == "getFromOffsets")(0)
    getFromOffsets.setAccessible(true)

    val kafkaUtilsInstance = KafkaUtils.getClass.getField("MODULE$").get(null)

    getFromOffsets.invoke(kafkaUtilsInstance, kafkaCluster.asInstanceOf[Object], kafkaParams,
      config.table.realTime.kafka.get.topics.toSet).asInstanceOf[Map[TopicAndPartition, Long]]
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

  class FileSystemOffsetStore(config: JobConf) extends OffsetStore(config) {

    val offsetPath = config.table.realTime.kafka.get.offsetStore.get.fsPath.get

    override protected def load(): Map[TopicAndPartition, Long] = {
      Try {
        logInfo(s"Loading Kafka offsets from: ${offsetPath}")
        deserializeOffsets(FileSystemUtil.getContent(offsetPath))
      }.getOrElse(Map())
    }

    override def save(offsets: Map[TopicAndPartition, Long]): Unit = {
      logInfo(s"Storing Kafka offsets to: ${offsetPath}")
      FileSystemUtil.setContent(offsetPath, serializeOffsets(offsets))
    }
  }

  class ConsulOffsetStore(config: JobConf) extends OffsetStore(config) {

    val key = s"${config.consulPrefix.stripSuffix("/")}/${config.table}/kafka/offsets"

    override protected def load(): Map[TopicAndPartition, Long] = {
      Try {
        logInfo(s"Loading Kafka offsets from Consul key: ${key}")
        deserializeOffsets(config.consulClient.kvGet(key))
      }.getOrElse(Map())
    }

    override def save(offsets: Map[TopicAndPartition, Long]): Unit = {
      logInfo(s"Storing Kafka offsets to Consul key: ${key}")
      config.consulClient.kvPut(key, serializeOffsets(offsets))
    }
  }

  def create(config: JobConf): OffsetStore = {
    config.table.realTime.kafka.get.offsetStore.getOrElse(new OffsetStoreConf).`type` match {
      case "fs" => new FileSystemOffsetStore(config)
      case "consul" => new ConsulOffsetStore(config)
      case _ => throw new IllegalArgumentException("Unknown Kafka offset store type!")
    }
  }
}
