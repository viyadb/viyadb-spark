package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.streaming.StreamingProcess.{MicroBatchInfo, MicroBatchOffsets}
import com.github.viyadb.spark.streaming.parser.Record
import com.github.viyadb.spark.util.KafkaUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{StreamingContext, Time}

/**
  * Streaming process based on events sent through Kafka
  */
class KafkaStreamingProcess(jobConf: JobConf) extends StreamingProcess(jobConf) {

  val kafkaConf = jobConf.indexer.realTime.kafkaSource.get

  /**
    * Dirty method (because of reflection use) of fetching latest offsets from Kafka brokers.
    *
    * @return Offsets per topic and partition
    */
  protected def latestOffests(): Map[TopicAndPartition, Long] = {
    logInfo("Fetching latest offsets from Kafka")
    KafkaUtil.latestOffsets(kafkaConf.brokers.mkString(","), kafkaConf.topics.toSet)
  }

  /**
    * Find offsets in latest notification, or retrieve latest offsets from target Kafka broker
    *
    * @return
    */
  protected def storedOrLatestOffsets(): Map[TopicAndPartition, Long] = {
    val latestOffsets = latestOffests()

    val storedOffsets = notifier.lastMessage.map { lastMesage =>
      lastMesage.offsets.get.map(offset => (TopicAndPartition(offset.topic, offset.partition), offset.offset)).toMap
    }.getOrElse(Map())

    latestOffsets.map { case (k, v) => k -> storedOffsets.getOrElse(k, v) }
  }

  override protected def createStream(ssc: StreamingContext): DStream[Record] = {
    val offsets = storedOrLatestOffsets()
    logInfo(s"Start consuming from offsets: ${offsets}")

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, Record](
      ssc,
      Map("metadata.broker.list" -> kafkaConf.brokers.mkString(",")),
      offsets,
      (m: MessageAndMetadata[String, String]) => recordParser.parseRecord(m.topic, m.message()).get)
  }

  override protected def createNotification(rdd: RDD[Record], time: Time): MicroBatchInfo = {
    val info = super.createNotification(rdd, time)

    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      .map(r => MicroBatchOffsets(r.topicAndPartition().topic, r.topicAndPartition().partition, r.untilOffset))

    MicroBatchInfo(id = info.id, tables = info.tables, offsets = Some(offsetRanges))
  }
}
