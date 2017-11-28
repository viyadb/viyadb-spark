package com.github.viyadb.spark.batch

import java.util.TimeZone

import com.github.viyadb.spark.Configs
import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.util.DirectOutputCommitter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entrance of the Spark batch application
  */
class Job {

  protected def appName() = "ViyaDB Batch"

  protected def kryoRegistrator(): Class[_] = {
    classOf[KryoRegistrator]
  }

  protected def sparkConf(): SparkConf = {
    new SparkConf().setAppName(appName())
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.registrator", kryoRegistrator().getName())
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.hadoop.mapred.output.committer.class", classOf[DirectOutputCommitter].getName)
  }

  protected def createSparkSession(jobConf: JobConf): SparkSession = {
    SparkSession.builder().config(sparkConf())
      .enableHiveSupport()
      .getOrCreate()
  }

  def run(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val config = Configs.readConfig(args)

    val spark = createSparkSession(config)
    new BatchProcess(config).start(spark)
  }
}

object Job {
  def main(args: Array[String]): Unit = {
    new Job().run(args)
  }
}
