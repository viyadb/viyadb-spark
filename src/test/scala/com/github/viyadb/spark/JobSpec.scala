package com.github.viyadb.spark

import java.io.File
import java.util.TimeZone

import com.github.viyadb.spark.batch.{Job => BatchJob}
import com.github.viyadb.spark.streaming.StreamingTestUtils.TestStreamingProcess
import com.github.viyadb.spark.streaming.{Job => StreamingJob}
import com.github.viyadb.spark.util.{ConsulClient, DummyStatsDServer}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.containers.GenericContainer

import scala.util.Random

class JobSpec extends UnitSpec with BeforeAndAfterAll {

  private var consul: GenericContainer[_] = _
  private var statsdServer: DummyStatsDServer = _

  override def beforeAll() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    consul = new GenericContainer("consul:latest")
      .withExposedPorts(8500)
    consul.start()

    statsdServer = new DummyStatsDServer(12345)
  }

  override def afterAll(): Unit = {
    if (consul != null) {
      consul.stop()
    }
    statsdServer.clear()
  }

  "Streaming and Batch jobs" should "run via main method" in {
    val consulClient = new ConsulClient(consul.getContainerIpAddress, consul.getFirstMappedPort)

    val tmpDir = File.createTempFile("viyadb-spark-test", null)
    tmpDir.delete()

    try {
      val suffix = Math.abs(Random.nextInt())
      consulClient.kvPut(s"/viyadb/indexers/indexer$suffix/config",
        s"""
           |{
           |  "tables":[
           |    "events$suffix"
           |  ],
           |  "statsd":{
           |    "host":"localhost",
           |    "port":12345,
           |    "prefix":"viyadb"
           |  },
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
           |      "queue":"rt-notifications$suffix"
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
           |      "queue":"b-notifications$suffix"
           |    }
           |  }
           |}
           |""".stripMargin)

      consulClient.kvPut(s"/viyadb/tables/events$suffix/config",
        s"""
           |{
           |  "name":"events$suffix",
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
        "--indexer-id", s"indexer$suffix"
      ))

      assert(!FileUtils.listFiles(
        new File(s"${tmpDir.getAbsolutePath}/deepStore/realtime/events$suffix"), Array("gz"), true).isEmpty)

      val batchJob = new BatchJob() {
        override protected def sparkConf(): SparkConf = {
          super.sparkConf().setMaster("local[*]")
        }
      }
      batchJob.run(Array(
        "--consul-host", consul.getContainerIpAddress,
        "--consul-port", consul.getFirstMappedPort.toString,
        "--consul-prefix", "viyadb",
        "--indexer-id", s"indexer$suffix"
      ))

      assert(!FileUtils.listFiles(
        new File(s"${tmpDir.getAbsolutePath}/deepStore/batch/events$suffix"), Array("gz"), true).isEmpty)

      Thread.sleep(1000L)
      assert(statsdServer.messages.exists(_.startsWith(s"viyadb.realtime.tables.events$suffix.save_time:")))
      assert(statsdServer.messages.exists(_.equals(s"viyadb.realtime.tables.events$suffix.saved_records:6|c")))
      assert(statsdServer.messages.exists(_.startsWith(s"viyadb.realtime.tables.events$suffix.process_time:")))
      assert(statsdServer.messages.exists(_.startsWith(s"viyadb.realtime.process_time:")))
      assert(statsdServer.messages.exists(_.startsWith(s"viyadb.batch.tables.events$suffix.process_time:")))
      assert(statsdServer.messages.exists(_.startsWith(s"viyadb.batch.tables.events$suffix.saved_records:2|c")))
      assert(statsdServer.messages.exists(_.startsWith(s"viyadb.batch.process_time:")))

    } finally {
      FileUtils.deleteDirectory(tmpDir)
    }
  }
}