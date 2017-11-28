package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.{JobConf, TableConf}
import com.github.viyadb.spark.notifications.Notifier
import com.github.viyadb.spark.processing.Processor
import com.github.viyadb.spark.streaming.StreamingProcess.{MicroBatchInfo, MicroBatchTableInfo}
import com.github.viyadb.spark.streaming.parser.{Record, RecordParser}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}


abstract class StreamingProcess(jobConf: JobConf) extends Serializable with Logging {

  lazy protected val recordParser: RecordParser = RecordParser.create(jobConf)

  lazy protected val processors: Map[String, Processor] = jobConf.tableConfigs.map(tableConf =>
    (tableConf.name, jobConf.indexer.realTime.processorClass.map(c =>
      Class.forName(c).getDeclaredConstructor(classOf[TableConf]).newInstance(tableConf).asInstanceOf[Processor]
    ).getOrElse(new StreamingProcessor(tableConf)))
  ).toMap

  lazy protected val saver: MicroBatchSaver = new MicroBatchSaver(jobConf.indexer)

  lazy protected val notifier: Notifier[MicroBatchInfo] = Notifier.create[MicroBatchInfo](jobConf.indexer.realTime.notifier)

  /**
    * Method for initializing DStream
    *
    * @param ssc Spark streaming context
    * @return new stream
    */
  protected def createStream(ssc: StreamingContext): DStream[Record]

  /**
    * This method is called prior to any processing in order to improve performance
    * when multiple operations are executed on a stream.
    */
  protected def cacheRDD(rdd: RDD[Record]): RDD[Record] = {
    rdd.persist(StorageLevel.MEMORY_ONLY_SER)
  }

  /**
    * This method is called when RDD of received batch is converted to a Data Frame.
    *
    * @param rdd RDD of input rows
    * @return data frame
    */
  protected def createDataFrame(rdd: RDD[Record]): DataFrame = {
    recordParser.createDataFrame(rdd)
  }

  /**
    * Main processing method for the table data frame.
    *
    * @param tableConf Table configuration
    * @param df        Data frame
    * @return Transformed data frame
    */
  protected def processDataFrame(tableConf: TableConf, df: DataFrame): DataFrame = {
    processors(tableConf.name).process(df)
  }

  /**
    * Main processing method for the input RDD, which is called once per received batch.
    *
    * @param rdd  Input RDD
    * @param time Batch timestamp
    */
  protected def processRDD(rdd: RDD[Record], time: Time): Unit = {
    val cachedRdd = cacheRDD(rdd)

    val df = createDataFrame(cachedRdd)

    jobConf.tableConfigs.foreach { tableConf =>
      val tableDf = processDataFrame(tableConf, df)
      saveDataFrame(tableConf, tableDf, time)
    }

    uncacheRDD(rdd)

    sendNotification(time, createNotification(rdd, time))
  }

  /**
    * This is the output operation for the received batch.
    *
    * @param tableConf Table configuration
    * @param df        Data frame that represents received batch
    * @param time      Batch timestamp
    */
  protected def saveDataFrame(tableConf: TableConf, df: DataFrame, time: Time): Unit = {
    saver.save(tableConf, df, time)
  }

  /**
    * This method removes batch RDD from cache once all the operations on it have been completed.
    *
    * @param rdd Batch RDD
    */
  protected def uncacheRDD(rdd: RDD[Record]): Unit = {
    rdd.unpersist(true)
  }


  /**
    * Creates a notification containing the micro-batch information
    */
  protected def createNotification(rdd: RDD[Record], time: Time): MicroBatchInfo = {
    val paths = saver.getWrittenPaths()
    val tablesInfo = jobConf.tableConfigs.map(tableConfig =>
      (tableConfig.name, MicroBatchTableInfo(paths = paths(tableConfig.name)))).toMap
    MicroBatchInfo(id = time.milliseconds, tables = tablesInfo)
  }

  /**
    * Sends the notification
    */
  protected def sendNotification(time: Time, info: MicroBatchInfo): Unit = {
    notifier.send(time.milliseconds, info)
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

object StreamingProcess {

  case class MicroBatchTableInfo(paths: Seq[String]) extends Serializable

  case class MicroBatchOffsets(topic: String, partition: Int, offset: Long) extends Serializable

  case class MicroBatchInfo(id: Long,
                            tables: Map[String, MicroBatchTableInfo],
                            offsets: Option[Seq[MicroBatchOffsets]] = None) extends Serializable

  def create(jobConf: JobConf): StreamingProcess = {
    jobConf.indexer.realTime.streamingProcessClass.map(c =>
      Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(jobConf).asInstanceOf[StreamingProcess]
    ).getOrElse(
      jobConf.indexer.realTime.kafkaSource.map(_ =>
        new KafkaStreamingProcess(jobConf)
      ).getOrElse(
        throw new IllegalArgumentException("No real-time source is specified!")
      )
    )
  }
}
