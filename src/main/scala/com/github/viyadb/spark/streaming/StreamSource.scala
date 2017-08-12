package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.processing.Processor
import com.github.viyadb.spark.streaming.kafka.KafkaStreamSource
import com.github.viyadb.spark.streaming.record.RecordFactory
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

  lazy protected val recordFactory = RecordFactory.create(config)

  lazy protected val processor = config.table.realTime.processorClass.map(c =>
    Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(config).asInstanceOf[Processor]
  ).getOrElse(new StreamingProcessor(config))

  lazy protected val saver = new MicroBatchSaver(config, recordFactory)

  /**
    * Method for initializing DStream
    *
    * @param ssc Spark streaming context
    * @return new stream
    */
  protected def createStream(ssc: StreamingContext): DStream[Row]

  /**
    * This method is called prior to any processing in order to improve performance
    * when multiple operations are executed on a stream.
    *
    * @param rdd
    * @return
    */
  protected def cacheRDD(rdd: RDD[Row]): RDD[Row] = {
    rdd.persist(StorageLevel.MEMORY_ONLY_SER)
  }

  /**
    * This method is called when RDD of received batch is converted to a Data Frame.
    *
    * @param rdd RDD of input rows
    * @return data frame
    */
  protected def createDataFrame(rdd: RDD[Row]): DataFrame = {
    recordFactory.createDataFrame(rdd)
  }

  /**
    * Main processing method for the data frame.
    *
    * @param df Data frame
    * @return Transformed data frame
    */
  protected def processDataFrame(df: DataFrame): DataFrame = {
    processor.process(df)
  }

  /**
    * Main processing method for the input RDD, which is called once per received batch.
    *
    * @param rdd  Input RDD
    * @param time Batch timestamp
    */
  protected def processRDD(rdd: RDD[Row], time: Time) = {
    val cachedRdd = cacheRDD(rdd)
    val df = createDataFrame(cachedRdd)
    saveDataFrame(processDataFrame(df), time)
    uncacheRDD(rdd)
  }

  /**
    * This is the output operation for the received batch.
    *
    * @param df   Data frame that represents received batch
    * @param time Batch timestamp
    */
  protected def saveDataFrame(df: DataFrame, time: Time) = {
    saver.save(df, time)
  }

  /**
    * This method removes batch RDD from cache once all the operations on it have been completed.
    *
    * @param rdd Batch RDD
    */
  protected def uncacheRDD(rdd: RDD[Row]): Unit = {
    rdd.unpersist(true)
  }

  /**
    * Initializes stream processing (main entry point)
    *
    * @param ssc Stream context
    */
  def start(ssc: StreamingContext): Unit = {
    createStream(ssc).foreachRDD { (rdd, time) =>
      if (rdd.toLocalIterator.nonEmpty) {
        processRDD(rdd, time)
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
