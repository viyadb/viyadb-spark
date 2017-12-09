package com.github.viyadb.spark

import com.github.viyadb.spark.util.{ConsulClient, JodaSerializers, TypeUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType
import org.joda.time.{Interval, Period}
import org.json4s.jackson.Serialization.{read, writePretty}
import org.json4s.{DefaultFormats, Formats}

/**
  * Indexer job configuration classes and parser utility
  */
object Configs extends Logging {

  /**
    * Base interface for table column (metric or dimension)
    */
  trait ColumnConf {

    /**
      * Input dataset field name, which may be different from target name
      */
    def inputField: String

    /**
      * Target field name
      */
    def name: String

    /**
      * Returns appropriate Spark data type
      */
    def dataType: DataType
  }

  /**
    * Table dimension configuration
    */
  case class DimensionConf(name: String,
                           field: Option[String] = None,
                           `type`: Option[String] = Some("string"),
                           max: Option[Long] = None,
                           format: Option[String] = None,
                           granularity: Option[String] = None) extends Serializable with ColumnConf {

    def inputField: String = field.getOrElse(name)

    def isTimeType: Boolean = `type`.exists(t => (t == "time") || (t == "microtime"))

    def dataType: DataType = TypeUtil.dataType(this)
  }

  /**
    * Table metric configuration
    */
  case class MetricConf(name: String,
                        field: Option[String] = None,
                        max: Option[Long] = None,
                        `type`: String) extends Serializable with ColumnConf {

    def inputField: String = field.getOrElse(name)

    def isBitsetType: Boolean = `type` == "bitset"

    def isCountType: Boolean = `type` == "count"

    def dataType: DataType = TypeUtil.dataType(this)
  }

  /**
    * Configures how data should be partitioned by the batch process
    */
  case class PartitionConf(columns: Seq[String],
                           partitions: Int,
                           hash: Option[Boolean] = Some(true)) extends Serializable

  /**
    * Table structure configuration
    */
  case class TableConf(name: String,
                       dimensions: Seq[DimensionConf],
                       metrics: Seq[MetricConf],
                       partitioning: Option[PartitionConf] = None,
                       sortColumns: Option[Seq[String]] = None) extends Serializable {

    def hasCountMetric: Boolean = metrics.exists(_.isCountType)
  }

  /**
    * Configures where processed batch info (by both real-time and batch indexers) will be sent to
    *
    * @param queue   Logical message queue name on which to send updates. In case of Kafka notifier, it's
    *                topic name. In case of File notifier, it's sub-directory where all messages will be written. Etc.
    * @param channel Message queue target host(s). In case of Kafka notifier, it's a comma-separated list of brokers.
    *                In case of File notifier, it's a base path where all messages will be written. Etc.
    * @param `type`  Notification message queue type (kafka, file, etc.).
    */
  case class NotifierConf(queue: String = "",
                          channel: String = "",
                          `type`: String = "unknown") extends Serializable

  case class KafkaSourceConf(topics: Seq[String],
                             brokers: Seq[String]) extends Serializable

  /**
    * Defines how incoming in real-time data is to be parsed
    */
  case class ParseSpecConf(format: String = "unknown",
                           columns: Option[Seq[String]] = None,
                           delimiter: Option[String] = None,
                           timeColumn: Option[String] = None,
                           fieldMapping: Option[Map[String, String]] = None,
                           timeFormats: Option[Map[String, String]] = None,
                           skipBadRows: Option[Boolean] = Some(false),
                           nullNumericAsZero: Option[Boolean] = Some(true),
                           nullStringAsEmpty: Option[Boolean] = Some(true),
                           recordParserClass: Option[String] = None) extends Serializable

  /**
    * Real-time process configuration
    */
  case class RealTimeConf(streamingProcessClass: Option[String] = None,
                          kafkaSource: Option[KafkaSourceConf] = None,
                          parseSpec: Option[ParseSpecConf] = None,
                          windowDuration: Option[Period] = None,
                          processorClass: Option[String] = None,
                          notifier: NotifierConf = NotifierConf()) extends Serializable {
  }

  /**
    * Batch process configuration
    */
  case class BatchConf(batchDuration: Option[Period] = None,
                       keepInterval: Option[Interval] = None,
                       partitioning: Option[PartitionConf] = None,
                       sortColumns: Option[Seq[String]] = None,
                       processorClass: Option[String] = None,
                       notifier: NotifierConf = NotifierConf()) extends Serializable {

    def batchDurationInMillis: Long = {
      batchDuration.getOrElse(Period.days(1)).toStandardSeconds.getSeconds * 1000L
    }
  }

  /**
    * Indexer configuration
    */
  case class IndexerConf(deepStorePath: String,
                         realTime: RealTimeConf,
                         batch: BatchConf,
                         tables: Seq[String] = Seq()) extends Serializable {
    /**
      * @return prefix under which batch process artifacts will be saved
      */
    def batchPrefix: String = {
      s"$deepStorePath/batch"
    }

    /**
      * @return prefix under which micro-batches will be saved
      */
    def realtimePrefix: String = {
      s"$deepStorePath/realtime"
    }
  }

  /**
    * Job configuration
    */
  case class JobConf(consulClient: ConsulClient = new ConsulClient(),
                     consulPrefix: String = "viyadb",
                     indexer: IndexerConf,
                     tableConfigs: Seq[TableConf]) extends Serializable

  private[spark] def parseConf[A](json: String)(implicit m: Manifest[A]): A = {
    implicit val formats: Formats = DefaultFormats ++ JodaSerializers.all
    read[A](json)
  }

  /**
    * Parses command line arguments, and reads job configuration from Consul
    *
    * @param args Command line arguments
    * @return job configuration
    */
  def readConfig(args: Array[String]): JobConf = {
    val cmdArgs = CmdArgs.parse(args)
    val consul = new ConsulClient(cmdArgs.consulHost, cmdArgs.consulPort, cmdArgs.consulToken)

    val indexerConf = parseConf[IndexerConf](
      consul.kvGet(s"${cmdArgs.consulPrefix}/indexers/${cmdArgs.indexerId}/config"))

    val tableConfigs = indexerConf.tables.map(name =>
      parseConf[TableConf](consul.kvGet(s"${cmdArgs.consulPrefix}/tables/$name/config")))

    val jobConf = JobConf(
      consulClient = consul,
      consulPrefix = cmdArgs.consulPrefix,
      indexer = indexerConf,
      tableConfigs = tableConfigs
    )

    implicit val formats = DefaultFormats
    logInfo(s"Using configuration: ${writePretty(jobConf)}")

    jobConf
  }
}
