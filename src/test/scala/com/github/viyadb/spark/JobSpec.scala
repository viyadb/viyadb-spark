package com.github.viyadb.spark

import java.io.File
import java.util.TimeZone

import com.github.viyadb.spark.batch.{Job => BatchJob}
import com.github.viyadb.spark.streaming.StreamingTestUtils.TestStreamingProcess
import com.github.viyadb.spark.streaming.{Job => StreamingJob}
import com.github.viyadb.spark.util.ConsulClient
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.containers.GenericContainer

class JobSpec extends UnitSpec with BeforeAndAfterAll {

  private var consul: GenericContainer[_] = _

  override def beforeAll() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    consul = new GenericContainer("consul:latest")
      .withExposedPorts(8500)
    consul.start()
  }

  override def afterAll(): Unit = {
    if (consul != null) {
      consul.stop()
    }
  }

  "Streaming Job" should "run" in {
    val consulClient = new ConsulClient(consul.getContainerIpAddress, consul.getFirstMappedPort)

    val tmpDir = File.createTempFile("viyadb-spark-test", null)
    tmpDir.delete()

    try {
      consulClient.kvPut("/viyadb/indexers/main/config",
        s"""
           |{
           |  "tables":[
           |    "events"
           |  ],
           |  "deepStorePath":"${tmpDir.getAbsolutePath}/deepStore",
           |  "realTime":{
           |    "windowDuration":"PT1S",
           |    "streamingProcessClass":"${classOf[TestStreamingProcess].getName}",
           |    "parseSpec":{
           |      "format":"tsv",
           |      "timeColumn":"timestamp",
           |      "columns":["company","timestamp","stock_price"],
           |      "timeFormats":{"timestamp":"%Y-%m-%d"}
           |    },
           |    "notifier":{
           |      "type":"file",
           |      "channel":"${tmpDir.getAbsolutePath}",
           |      "queue":"rt-notifications"
           |    }
           |  },
           |  "batch":{
           |    "partitioning":{
           |      "columns":[
           |        "company"
           |      ],
           |      "partitions":3
           |    },
           |    "notifier":{
           |      "type":"file",
           |      "channel":"${tmpDir.getAbsolutePath}",
           |      "queue":"b-notifications"
           |    }
           |  }
           |}
           |""".stripMargin)

      consulClient.kvPut("/viyadb/tables/events/config",
        """
          |{
          |  "name":"events",
          |  "dimensions":[
          |     {"name":"company"},
          |     {"name":"timestamp","type":"time","format":"%Y-%m-%d"}
          |   ],
          |   "metrics":[
          |     {"name":"stock_price_sum","field":"stock_price","type":"double_sum"},
          |     {"name":"stock_price_avg","field":"stock_price","type":"double_avg"},
          |     {"name":"stock_price_max","field":"stock_price","type":"double_max"},
          |     {"name":"count","type":"count"}
          |   ]
          |}
          |""".stripMargin)

      val streamingJob = new StreamingJob() {
        override protected def sparkConf(): SparkConf = {
          super.sparkConf().setMaster("local[*]")
        }

        override def startAndAwaitTermination(ssc: StreamingContext): Unit = {
          ssc.start()
          ssc.stop(stopSparkContext = false, stopGracefully = true)
          ssc.awaitTermination()
        }
      }
      streamingJob.run(Array(
        "--consul-host", consul.getContainerIpAddress,
        "--consul-port", consul.getFirstMappedPort.toString,
        "--consul-prefix", "viyadb",
        "--indexer-id", "main"
      ))

      assert(!FileUtils.listFiles(
        new File(s"${tmpDir.getAbsolutePath}/deepStore/realtime/events"), Array("gz"), true).isEmpty)

      val batchJob = new BatchJob() {
        override protected def sparkConf(): SparkConf = {
          super.sparkConf().setMaster("local[*]")
        }
      }
      batchJob.run(Array(
        "--consul-host", consul.getContainerIpAddress,
        "--consul-port", consul.getFirstMappedPort.toString,
        "--consul-prefix", "viyadb",
        "--indexer-id", "main"
      ))

      assert(!FileUtils.listFiles(
        new File(s"${tmpDir.getAbsolutePath}/deepStore/batch/events"), Array("gz"), true).isEmpty)

    } finally {
      FileUtils.deleteDirectory(tmpDir)
    }
  }
}