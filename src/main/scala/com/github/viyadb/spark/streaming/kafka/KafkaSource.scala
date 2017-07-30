package com.github.viyadb.spark.streaming.kafka

import com.github.viyadb.spark.TableConfig
import com.github.viyadb.spark.processing.Processor
import com.github.viyadb.spark.streaming.Source
import com.github.viyadb.spark.streaming.message.MessageFactory
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

class KafkaSource(table: TableConfig.Table) extends Source {

  @transient
  lazy val log = org.apache.log4j.Logger.getLogger(getClass)

  val kafkaSpec = table.realTime.kafka.get

  val offsetStore = OffsetStore.create(kafkaSpec)

  lazy val messageFactory = MessageFactory.create(table)

  lazy val processor = Processor.create(table)

  override def start(ssc: StreamingContext): Unit = {

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, AnyRef](
      ssc,
      Map("metadata.broker.list" -> kafkaSpec.brokers.mkString(",")),
      offsetStore.loadOrFetch(),
      (m: MessageAndMetadata[String, String]) => messageFactory.handleMessage(m.topic, m.message()))

    kafkaStream.foreachRDD { (rdd, time) =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (rdd.toLocalIterator.nonEmpty) {
        rdd.persist(StorageLevel.MEMORY_ONLY_SER)
        processor.process(messageFactory.createDataFrame(rdd))
        rdd.unpersist(true)
        offsetStore.save(offsetRanges.map(r => (r.topicAndPartition(), r.untilOffset)).toMap)
      }
    }
  }
}
