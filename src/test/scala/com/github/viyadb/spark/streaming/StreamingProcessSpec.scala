package com.github.viyadb.spark.streaming

import java.io.File
import java.util.TimeZone

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import com.github.viyadb.spark.streaming.StreamingProcess.MicroBatchInfo
import com.github.viyadb.spark.streaming.StreamingProcessSpec.TestStreamingProcess
import com.github.viyadb.spark.streaming.parser.{Record, RecordParser}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.scalatest.BeforeAndAfter

object StreamingProcessSpec {

  class TestRDD(@transient val _sc: SparkContext, val recordParser: RecordParser)
    extends RDD[Record](_sc, Nil) {

    override def compute(split: Partition, context: TaskContext): Iterator[Record] = {
      Seq(
        Seq("IBM", "2015-01-01", "101.1").mkString("\t"),
        Seq("IBM", "2015-01-01", "102.32").mkString("\t"),
        Seq("IBM", "2015-01-02", "105.0").mkString("\t"),
        Seq("IBM", "2015-01-02", "99.7").mkString("\t"),
        Seq("IBM", "2015-01-03", "98.12").mkString("\t"),
        Seq("Amdocs", "2015-01-01", "50.0").mkString("\t"),
        Seq("Amdocs", "2015-01-01", "57.14").mkString("\t"),
        Seq("Amdocs", "2015-01-02", "89.22").mkString("\t"),
        Seq("Amdocs", "2015-01-02", "90.3").mkString("\t"),
        Seq("Amdocs", "2015-01-03", "1.01").mkString("\t")
      )
        .map(recordParser.parseRecord("", _).get).toIterator
    }

    override protected def getPartitions: Array[Partition] = {
      Array(new Partition {
        override def index: Int = 0
      })
    }
  }

  class TestInputDStream(ssc_ : StreamingContext, val recordParser: RecordParser)
    extends InputDStream[Record](ssc_) {

    var sent = false

    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def compute(validTime: Time): Option[RDD[Record]] = {
      if (sent) {
        None
      } else {
        sent = true
        Some(new TestRDD(ssc_.sparkContext, recordParser))
      }
    }
  }

  class TestStreamingProcess(jobConf: JobConf) extends StreamingProcess(jobConf) {
    override protected def createStream(ssc: StreamingContext): DStream[Record] = {
      new TestInputDStream(ssc, recordParser)
    }

    override protected def saveDataFrame(tableConf: TableConf, df: DataFrame, time: Time): Unit = {
      super.saveDataFrame(tableConf, df.coalesce(1), time)
    }
  }

}

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
      ssc.stop(false, true)
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
}