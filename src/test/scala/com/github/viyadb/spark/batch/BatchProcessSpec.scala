package com.github.viyadb.spark.streaming

import java.io.File
import java.util.TimeZone

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import com.github.viyadb.spark.batch.BatchProcess
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter

class BatchProcessSpec extends UnitSpec with BeforeAndAfter {

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

  "BatchProcess" should "process realtime data" in {
    val tmpDir = File.createTempFile("viyadb-spark-test", null)
    tmpDir.delete()

    try {
      FileUtils.copyDirectory(new File(getClass.getResource("/deepStore").getPath), tmpDir)

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
          notifier = NotifierConf(
            `type` = "file",
            channel = tmpDir.getAbsolutePath,
            queue = "notifications"
          )
        ),
        batch = BatchConf(
          notifier = NotifierConf(
            `type` = "file",
            channel = tmpDir.getAbsolutePath,
            queue = "batch-notifications"
          ),
          partitioning = Some(PartitionConf(
            columns = Seq("company"),
            hash = Some(false),
            partitions = 2
          ))
        )
      )

      val jobConf = JobConf(
        indexer = indexerConf,
        tableConfigs = Seq(tableConf)
      )

      new BatchProcess(jobConf).start(ss)

      val actual = ss.sparkContext.textFile(jobConf.indexer.batchPrefix + "/foo/*/*/*.gz")
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
}
