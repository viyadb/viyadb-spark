package com.github.viyadb.spark.streaming

import com.esotericsoftware.kryo.Kryo
import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.processing.{Aggregator, Processor, ProcessorChain}
import com.github.viyadb.spark.streaming.kafka.OffsetStore.{ConsulOffsetStore, FileSystemOffsetStore}
import com.github.viyadb.spark.streaming.kafka.{KafkaStreamSource, OffsetStore}
import com.github.viyadb.spark.streaming.message.{JsonMessageFactory, Message, MessageFactory, TsvMessageFactory}
import com.github.viyadb.spark.util.ConsulClient
import org.apache.spark.streaming.receiver.Receiver
import org.joda.time.{DurationFieldType, Interval, Period, PeriodType}

class KryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[ConsulClient])
    kryo.register(classOf[JobConf])
    kryo.register(classOf[TableConf])
    kryo.register(classOf[BatchConf])
    kryo.register(classOf[MetricConf])
    kryo.register(classOf[DimensionConf])
    kryo.register(classOf[RealTimeConf])
    kryo.register(classOf[TimeColumnConf])
    kryo.register(classOf[ParseSpecConf])
    kryo.register(classOf[KafkaConf])
    kryo.register(classOf[OffsetStoreConf])
    kryo.register(classOf[Processor])
    kryo.register(classOf[StreamingProcessor])
    kryo.register(classOf[ProcessorChain])
    kryo.register(classOf[Aggregator])
    kryo.register(classOf[OffsetStore])
    kryo.register(classOf[FileSystemOffsetStore])
    kryo.register(classOf[ConsulOffsetStore])
    kryo.register(classOf[StreamSource])
    kryo.register(classOf[KafkaStreamSource])
    kryo.register(classOf[Message])
    kryo.register(classOf[MessageFactory])
    kryo.register(classOf[JsonMessageFactory])
    kryo.register(classOf[TsvMessageFactory])
    kryo.register(classOf[Array[Receiver[_]]])

    kryo.register(classOf[Period])
    kryo.register(classOf[PeriodType])
    kryo.register(classOf[Interval])
    kryo.register(classOf[Array[DurationFieldType]])
    kryo.register(Class.forName("org.joda.time.DurationFieldType$StandardDurationFieldType"))

    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Array[org.apache.spark.sql.catalyst.InternalRow]])
    kryo.register(classOf[Array[org.apache.spark.sql.Row]])

    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
  }
}