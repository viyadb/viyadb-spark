package com.github.viyadb.spark

import com.github.viyadb.spark.util.{ConsulClient, JodaSerializers}
import org.apache.spark.internal.Logging
import org.joda.time.{Interval, Period}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

/**
  * Table configuration classes and parser utility
  */
object Configs extends Logging {

  case class OffsetStoreConf(`type`: String = "consul", fsPath: Option[String] = None) extends Serializable

  case class KafkaConf(topics: Seq[String],
                       brokers: Seq[String],
                       offsetStore: Option[OffsetStoreConf] = None) extends Serializable

  case class ParseSpecConf(format: String,
                           delimiter: Option[String] = None,
                           columns: Option[Seq[String]] = None,
                           fieldMapping: Option[Map[String, String]] = None,
                           skipBadRows: Option[Boolean] = Some(false),
                           nullNumericAsZero: Option[Boolean] = Some(true),
                           nullStringAsEmpty: Option[Boolean] = Some(true)) extends Serializable

  case class TimeColumnConf(name: String = "timestamp", format: Option[String] = None) extends Serializable

  case class RealTimeConf(streamSourceClass: Option[String] = None,
                          kafka: Option[KafkaConf] = None,
                          parseSpec: Option[ParseSpecConf] = None,
                          windowDuration: Option[Period] = None,
                          recordFactoryClass: Option[String] = None,
                          processorClass: Option[String] = None) extends Serializable

  case class DimensionConf(name: String,
                           `type`: Option[String] = Some("string"),
                           max: Option[Long] = None,
                           format: Option[String] = None,
                           granularity: Option[String] = None) extends Serializable {

    def isTimeType(): Boolean = {
      `type`.exists(t => (t == "time") || (t == "microtime"))
    }
  }

  case class MetricConf(name: String,
                        max: Option[Long] = None,
                        `type`: String) extends Serializable

  case class PartitionConf(column: String,
                           numPartitions: Int,
                           hashColumn: Option[Boolean] = Some(false)) extends Serializable

  case class BatchConf(batchDuration: Option[Period] = None,
                       keepInterval: Option[Interval] = None,
                       partitioning: Option[PartitionConf] = None,
                       sortColumns: Option[Seq[String]] = None,
                       processorClass: Option[String] = None) extends Serializable {

    def batchDurationInMillis(): Long = {
      batchDuration.getOrElse(Period.days(1)).toStandardSeconds.getSeconds * 1000L
    }
  }

  case class TableConf(name: String,
                       realTime: RealTimeConf,
                       batch: BatchConf,
                       dimensions: Seq[DimensionConf],
                       metrics: Seq[MetricConf],
                       timeColumn: Option[TimeColumnConf] = None,
                       deepStorePath: String) extends Serializable

  case class JobConf(consulClient: ConsulClient = new ConsulClient(),
                     consulPrefix: String = "viyadb-cluster", table: TableConf) extends Serializable {

    /**
      * @return prefix under which batch process artifacts will be saved
      */
    def batchPrefix(): String = {
      s"${table.deepStorePath}/batch"
    }

    /**
      * @return prefix under which micro-batches will be saved
      */
    def realtimePrefix(): String = {
      s"${table.deepStorePath}/realtime"
    }
  }

  private[spark] def parseTableConf(json: String): TableConf = {
    implicit val formats = DefaultFormats ++ JodaSerializers.all
    read[TableConf](json)
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
    val config = JobConf(
      consulClient = consul,
      consulPrefix = cmdArgs.consulPrefix,
      table = parseTableConf(consul.kvGet(s"${cmdArgs.consulPrefix}/tables/${cmdArgs.table}/config"))
    )
    logInfo(s"Using configuration: ${config}")
    if (config.table.name != cmdArgs.table) {
      throw new IllegalArgumentException("Command line --table argument doesn't match the configuration")
    }
    config
  }
}
