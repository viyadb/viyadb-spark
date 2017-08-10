package com.github.viyadb.spark

import com.github.viyadb.spark.util.{ConsulClient, JodaSerializers}
import org.joda.time.{Interval, Period}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

/**
  * Table configuration classes and parser utility
  */
object Configs {

  case class OffsetStoreConf(`type`: String = "consul", fsPath: String) extends Serializable

  case class KafkaConf(topics: Seq[String], brokers: Seq[String], offsetStore: OffsetStoreConf) extends Serializable

  case class ParseSpecConf(format: String,
                           delimiter: Option[String] = None,
                           columns: Option[Seq[String]] = None,
                           fieldMapping: Option[Map[String, String]] = None,
                           skipBadRows: Boolean = false,
                           nullNumericAsZero: Boolean = true,
                           nullStringAsEmpty: Boolean = true) extends Serializable

  case class TimeColumnConf(name: String = "timestamp", format: Option[String] = None) extends Serializable

  case class RealTimeConf(streamSourceClass: Option[String] = None,
                          kafka: Option[KafkaConf] = None,
                          parseSpec: Option[ParseSpecConf] = None,
                          windowDuration: Option[Period] = None,
                          messageFactoryClass: Option[String] = None) extends Serializable

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

  case class BatchConf(batchDuration: Option[Period] = None,
                       keepInterval: Option[Interval] = None) extends Serializable

  case class TableConf(name: String,
                       realTime: RealTimeConf,
                       batch: BatchConf,
                       processorClass: Option[String] = None,
                       dimensions: Seq[DimensionConf],
                       metrics: Seq[MetricConf],
                       timeColumn: Option[TimeColumnConf] = None,
                       deepStorePath: String) extends Serializable

  case class JobConf(consulClient: ConsulClient = new ConsulClient(),
                     consulPrefix: String = "viyadb-cluster", table: TableConf) extends Serializable

  def parseTableConf(json: String): TableConf = {
    implicit val formats = DefaultFormats ++ JodaSerializers.all
    read[TableConf](json)
  }
}
