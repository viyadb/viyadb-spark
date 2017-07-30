package com.github.viyadb.spark.streaming.kafka

import com.github.viyadb.spark.TableConfig
import com.github.viyadb.spark.util.FileSystemUtil
import kafka.common.TopicAndPartition

import scala.util.Try

class FileSystemOffsetStore(kafka: TableConfig.Kafka) extends OffsetStore(kafka) {

  val offsetPath = kafka.offsetPath.get

  override protected def load(): Map[TopicAndPartition, Long] = {
    Try {
      log.info(s"Loading Kafka offsets from ${kafka.offsetPath}")
      FileSystemUtil.getContent(offsetPath).split("\n")
        .map(_.split(",")).filter(_.length == 3)
        .map { args => (TopicAndPartition(args(0), args(1).toInt), args(2).toLong) }.toMap
    }.getOrElse(Map())
  }

  override def save(offsets: Map[TopicAndPartition, Long]): Unit = {
    log.info(s"Storing Kafka offsets to ${offsetPath}")
    FileSystemUtil.setContent(offsetPath,
      offsets.map(o => s"${o._1.topic},${o._1.partition},${o._2}").mkString("\n"))
  }
}
