package com.github.viyadb.spark.streaming

import java.io.File
import java.util.TimeZone

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import com.github.viyadb.spark.notifications.Notifier
import com.github.viyadb.spark.streaming.StreamingTestUtils.TestStreamingProcess
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.BeforeAndAfter

class StreamingProcessSpec extends UnitSpec with BeforeAndAfter {

  private var ss: SparkSession = _

  before {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    ss = SparkSession.builder().appName(getClass.getName)
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (ss != null) {
      ss.stop()
    }
  }

  "StreamingProcess" should "process field reference" in {
    val tmpDir = File.createTempFile("viyadb-spark-test", null)
    tmpDir.delete()

    try {
      val tableConf = TableConf(
        name = "foo",
        dimensions = Seq(
          DimensionConf(name = "company"),
          DimensionConf(name = "timestamp", `type` = Some("time"), format = Some("%Y-%m-%d"))
        ),
        metrics = Seq(
          MetricConf(name = "stock_price_sum", field = Some("stock_price"), `type` = "double_sum"),
          MetricConf(name = "stock_price_avg", field = Some("stock_price"), `type` = "double_avg"),
          MetricConf(name = "stock_price_max", field = Some("stock_price"), `type` = "double_max"),
          MetricConf(name = "count", `type` = "count")
        )
      )

      val indexerConf = IndexerConf(
        deepStorePath = tmpDir.getAbsolutePath,
        realTime = RealTimeConf(
          parseSpec = Some(ParseSpecConf(
            format = "tsv",
            columns = Some(Seq("company", "timestamp", "stock_price")),
            timeColumn = Some("timestamp"),
            timeFormats = Some(Map("timestamp" -> "%Y-%m-%d"))
          )),
          streamingProcessClass = Some(classOf[TestStreamingProcess].getName),
          notifier = NotifierConf(
            `type` = "file",
            channel = tmpDir.getAbsolutePath,
            queue = "notifications"
          )
        ),
        batch = BatchConf()
      )

      val jobConf = JobConf(
        indexer = indexerConf,
        tableConfigs = Seq(tableConf)
      )

      val ssc = new StreamingContext(ss.sparkContext, Seconds(1))

      StreamingProcess.create(jobConf).start(ssc)

      ssc.start()
      ssc.stop(stopSparkContext = false, stopGracefully = true)
      ssc.awaitTermination()

      val actual = ss.sparkContext.textFile(jobConf.indexer.realtimePrefix + "/foo/*/*/*.gz")
        .collect().sorted.mkString("\n")

      val expected = Array(
        Array("Amdocs", "2015-01-01", "107.14", "107.14", "57.14", "2").mkString("\t"),
        Array("IBM", "2015-01-02", "204.7", "204.7", "105.0", "2").mkString("\t"),
        Array("IBM", "2015-01-03", "98.12", "98.12", "98.12", "1").mkString("\t"),
        Array("Amdocs", "2015-01-03", "1.01", "1.01", "1.01", "1").mkString("\t"),
        Array("Amdocs", "2015-01-02", "179.51999999999998", "179.51999999999998", "90.3", "2").mkString("\t"),
        Array("IBM", "2015-01-01", "203.42", "203.42", "102.32", "2").mkString("\t")
      )
        .sorted.mkString("\n")

      assert(actual == expected)

    } finally {
      FileUtils.deleteDirectory(tmpDir)
    }
  }

  "FileNotifier" should "support sending and receiving messages" in {
    val tmpDir = File.createTempFile("viyadb-spark-test", null)
    tmpDir.delete()

    try {
      val indexerConf = IndexerConf(
        deepStorePath = "",
        realTime = RealTimeConf(
          notifier = NotifierConf(
            `type` = "file",
            channel = tmpDir.getAbsolutePath,
            queue = "notifications"
          )
        ),
        batch = BatchConf()
      )
      val jobConf = JobConf(
        indexer = indexerConf,
        tableConfigs = Seq()
      )

      val fileNotifier = Notifier.create[String](jobConf, indexerConf.realTime.notifier)
      fileNotifier.send(1, "hello")
      fileNotifier.send(2, "world")

      assert(fileNotifier.lastMessage.get == "world")
      assert(fileNotifier.allMessages == Seq("hello", "world"))

      fileNotifier.send(3, "again")
      assert(fileNotifier.lastMessage.get == "again")
      assert(fileNotifier.allMessages == Seq("hello", "world", "again"))
    } finally {
      FileUtils.deleteDirectory(tmpDir)
    }
  }
}