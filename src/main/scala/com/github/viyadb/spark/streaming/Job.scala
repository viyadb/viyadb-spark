package com.github.viyadb.spark.streaming

import java.net.InetAddress

import com.github.viyadb.spark.Configs.{JobConf, parseTableConf}
import com.github.viyadb.spark.util.{ConsulClient, DirectOutputCommitter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Main entrance of the Spark streaming application
  */
class Job {

  case class Arguments(consulHost: InetAddress = InetAddress.getLocalHost, consulPort: Int = 8500,
                       consulToken: Option[String] = None, consulPrefix: String = "viyadb-cluster",
                       table: String = "")

  private def parseArguments(args: Array[String]) = {
    new scopt.OptionParser[Arguments]("spark-sample") {

      opt[InetAddress]("consul-host").optional().action((x, c) =>
        c.copy(consulHost = x)).text("Consul URL (default: localhost)")

      opt[Int]("consul-port").optional().action((x, c) =>
        c.copy(consulPort = x)).text("Consul port number (default: 8500)")

      opt[String]("consul-token").optional().action((x, c) =>
        c.copy(consulToken = Some(x))).text("Consul token if required")

      opt[String]("consul-prefix").optional().action((x, c) =>
        c.copy(consulToken = Some(x))).text("Consul key-value prefix path (default: viyadb-cluster)")

      opt[String]("table").action((x, c) =>
        c.copy(table = x)).text("Name of the table to process")

      help("help").text("prints this usage text")

    }.parse(args, Arguments()) match {
      case Some(config) => config
      case None => throw new RuntimeException("Wrong usage")
    }
  }

  protected def appName() = "ViyaDB Streaming"

  protected def kryoRegistrator(): Class[_] = {
    classOf[KryoRegistrator]
  }

  protected def sparkConf(): SparkConf = {
    new SparkConf().setAppName(appName())
      .set("spark.cleaner.ttl", "3600")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.registrator", kryoRegistrator().getName())
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.hadoop.mapred.output.committer.class", classOf[DirectOutputCommitter].getName)
  }

  protected def createStreamingContext(config: JobConf): StreamingContext = {
    val spark = SparkSession.builder().config(sparkConf())
      .enableHiveSupport()
      .getOrCreate()

    new StreamingContext(spark.sparkContext,
      config.table.realTime.windowDuration.map(p => Seconds(p.toStandardSeconds.getSeconds))
        .getOrElse(Seconds(5)))
  }

  protected def readConfig(args: Arguments): JobConf = {
    val consul = new ConsulClient(args.consulHost.getHostName, args.consulPort, args.consulToken)
    JobConf(
      consulClient = consul,
      consulPrefix = args.consulPrefix,
      table = parseTableConf(consul.kvGet(s"${args.consulPrefix}/${args.table}/table"))
    )
  }

  def run(args: Array[String]): Unit = {
    val config = readConfig(parseArguments(args))
    val ssc = createStreamingContext(config)

    StreamSource.create(config).start(ssc)

    ssc.start()
    ssc.awaitTermination()
  }
}

object Job {
  def main(args: Array[String]): Unit = {
    new Job().run(args)
  }
}
