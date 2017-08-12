package com.github.viyadb.spark.streaming

import java.util.TimeZone

import com.github.viyadb.spark.Configs
import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.util.DirectOutputCommitter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Main entrance of the Spark streaming application
  */
class Job {
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

  def run(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val config = Configs.readConfig(args)
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
