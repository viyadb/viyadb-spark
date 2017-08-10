package com.github.viyadb.spark.streaming.kafka

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.streaming.StreamSource
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

class KafkaStreamSource(config: JobConf) extends StreamSource(config) {

  @transient
  lazy val log = org.apache.log4j.Logger.getLogger(getClass)

  val kafkaConf = config.table.realTime.kafka.get

  val offsetStore = OffsetStore.create(config)

  override protected def createStream(ssc: StreamingContext): DStream[Row] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, Row](
      ssc,
      Map("metadata.broker.list" -> kafkaConf.brokers.mkString(",")),
      offsetStore.loadOrFetch(),
      (m: MessageAndMetadata[String, String]) => messageFactory.createMessage(m.topic, m.message()).get)
  }

  override protected def uncacheRDD(rdd: RDD[Row]): Unit = {
    super.uncacheRDD(rdd)

    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetStore.save(offsetRanges.map(r => (r.topicAndPartition(), r.untilOffset)).toMap)
  }
}
