package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.processing.Processor
import com.github.viyadb.spark.streaming.kafka.KafkaStreamSource
import com.github.viyadb.spark.streaming.message.MessageFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}

/**
  * Abstract stream source and generic methods for events processing
  *
  * @param config Job configuration
  */
abstract class StreamSource(config: JobConf) {

  @transient
  lazy protected val messageFactory = MessageFactory.create(config)

  @transient
  lazy protected val processor = Processor.create(config).getOrElse(new StreamingProcessor(config))

  protected def createStream(ssc: StreamingContext): DStream[Row]

  protected def preProcess(rdd: RDD[Row]): RDD[Row] = {
    rdd.persist(StorageLevel.MEMORY_ONLY_SER)
  }

  protected def process(rdd: RDD[Row], time: Time) = {
    preProcess(rdd)

    val df = messageFactory.createDataFrame(rdd)
    df.transform(processor.process)
    save(df, time)

    postProcess(rdd)
  }

  protected def save(df: DataFrame, time: Time) = {
    df.write
      .option("delimiter", "\t")
      .option("header", "false")
      .csv(s"${config.table.realTime.outputPath}/${time.milliseconds}")
  }

  protected def postProcess(rdd: RDD[Row]): Unit = {
    rdd.unpersist(true)
  }

  def start(ssc: StreamingContext): Unit = {
    createStream(ssc).foreachRDD { (rdd, time) =>
      if (rdd.toLocalIterator.nonEmpty) {
        process(rdd, time)
      }
    }
  }
}

object StreamSource {
  def create(config: JobConf): StreamSource = {
    config.table.realTime.streamSourceClass.map(c =>
      Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(config).asInstanceOf[StreamSource]
    ).getOrElse(
      config.table.realTime.kafka.map(_ =>
        new KafkaStreamSource(config)
      ).getOrElse(
        throw new IllegalArgumentException("No real-time source is specified!")
      )
    )
  }
}
