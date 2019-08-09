package com.github.viyadb.spark.batch

import com.esotericsoftware.kryo.Kryo
import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.notifications.{FileNotifier, KafkaNotifier, Notifier}
import com.github.viyadb.spark.processing.{Aggregator, Processor, ProcessorChain}
import com.github.viyadb.spark.streaming.{PathTracker, StreamingProcess}
import com.github.viyadb.spark.util.ConsulClient
import org.joda.time.{DurationFieldType, Interval, Period, PeriodType}

class KryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[ConsulClient])
    kryo.register(classOf[JobConf])
    kryo.register(classOf[IndexerConf])
    kryo.register(classOf[TableConf])
    kryo.register(classOf[BatchConf])
    kryo.register(classOf[PartitionConf])
    kryo.register(classOf[MetricConf])
    kryo.register(classOf[DimensionConf])
    kryo.register(classOf[RealTimeConf])
    kryo.register(classOf[ParseSpecConf])
    kryo.register(classOf[KafkaSourceConf])
    kryo.register(classOf[Processor])
    kryo.register(classOf[BatchProcessor])
    kryo.register(classOf[OutputSchema])
    kryo.register(classOf[ProcessorChain])
    kryo.register(classOf[Aggregator])
    kryo.register(classOf[PathTracker])
    kryo.register(classOf[MicroBatchLoader])
    kryo.register(classOf[Notifier[_]])
    kryo.register(classOf[KafkaNotifier[_]])
    kryo.register(classOf[FileNotifier[_]])
    kryo.register(classOf[StreamingProcess.MicroBatchInfo])
    kryo.register(classOf[StreamingProcess.MicroBatchOffsets])
    kryo.register(classOf[StreamingProcess.MicroBatchTableInfo])
    kryo.register(classOf[Array[StreamingProcess.MicroBatchInfo]])
    kryo.register(classOf[BatchProcess.BatchInfo])
    kryo.register(classOf[BatchProcess.BatchTableInfo])

    kryo.register(classOf[Period])
    kryo.register(classOf[PeriodType])
    kryo.register(classOf[Interval])
    kryo.register(classOf[Array[DurationFieldType]])
    kryo.register(Class.forName("org.joda.time.DurationFieldType$StandardDurationFieldType"))

    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Array[Byte]]])
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Array[org.apache.spark.sql.catalyst.InternalRow]])
    kryo.register(classOf[Array[org.apache.spark.sql.Row]])

    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
  }
}