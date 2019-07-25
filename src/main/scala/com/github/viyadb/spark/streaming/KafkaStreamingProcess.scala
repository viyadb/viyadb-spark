package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs
import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.streaming.StreamingProcess.{MicroBatchInfo, MicroBatchOffsets}
import com.github.viyadb.spark.streaming.parser.Record
import com.github.viyadb.spark.util.KafkaOffsetUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{StreamingContext, Time}


/**
  * Streaming process based on events sent through Kafka
  */
class KafkaStreamingProcess(jobConf: JobConf) extends StreamingProcess(jobConf) {

  val kafkaConf: Configs.KafkaSourceConf = jobConf.indexer.realTime.kafkaSource.get

  /**
    * Find offsets in latest notification, or retrieve latest offsets from target Kafka broker
    *
    * @return
    */
  protected def storedOrLatestOffsets(): Map[TopicPartition, Long] = {
    val kafkaOffsets =
      KafkaOffsetUtils.fetchOffsets(kafkaConf.brokers.mkString(","),
        kafkaConf.topics.toSet, kafkaConf.earliest.getOrElse(true))

    val storedOffsets = notifier.lastMessage.map { lastMesage =>
      lastMesage.offsets.get.map(offset => (new TopicPartition(offset.topic, offset.partition), offset.offset)).toMap
    }.getOrElse(Map())

    kafkaOffsets.map { case (topicPartition, offset) =>
      (topicPartition, storedOffsets.getOrElse(topicPartition, offset))
    }
  }

  override protected def foreachStreamRDD(ssc: StreamingContext,
                                          handler: (RDD[Record], Time, Any) => Unit): Unit = {
    val offsets = storedOrLatestOffsets()
    logInfo(s"Start consuming from offsets: $offsets")

    val consumerConf = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConf.brokers.mkString(","),
      ConsumerConfig.GROUP_ID_CONFIG -> jobConf.indexer.id,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](offsets.map(_._1.topic()), consumerConf, offsets))

    stream.foreachRDD { (rdd, time) =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        .map(r => MicroBatchOffsets(r.topicPartition().topic, r.topicPartition().partition, r.untilOffset))

      val recordRdd = rdd.map { record =>
        recordParser.parseRecord(record.topic(), record.value()).orNull
      }.filter(_ != null)

      handler(recordRdd, time, offsetRanges)
    }
  }

  override protected def createNotification(rdd: RDD[Record], time: Time, context: Any = null): MicroBatchInfo = {
    val info = super.createNotification(rdd, time)

    val offsets = context.asInstanceOf[Array[MicroBatchOffsets]]

    MicroBatchInfo(id = info.id, tables = info.tables, offsets = Some(offsets))
  }
}
