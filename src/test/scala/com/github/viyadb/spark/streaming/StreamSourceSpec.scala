package com.github.viyadb.spark.streaming

import java.io.File
import java.util.TimeZone

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import com.github.viyadb.spark.streaming.StreamSourceSpec.{TestInputDStream, TestStreamSource}
import com.github.viyadb.spark.streaming.record.RecordFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.scalatest.BeforeAndAfter

object StreamSourceSpec {

  class TestRDD(@transient val _sc: SparkContext, val recordFactory: RecordFactory)
    extends RDD[Row](_sc, Nil) {

    override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
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
        .map(recordFactory.createRecord("", _).get).toIterator
    }

    override protected def getPartitions: Array[Partition] = {
      Array(new Partition {
        override def index: Int = 0
      })
    }
  }

  class TestInputDStream(ssc_ : StreamingContext, val recordFactory: RecordFactory)
    extends InputDStream[Row](ssc_) {

    var sent = false

    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def compute(validTime: Time): Option[RDD[Row]] = {
      if (sent) {
        None
      } else {
        sent = true
        Some(new TestRDD(ssc_.sparkContext, recordFactory))
      }
    }
  }

  class TestStreamSource(config: JobConf) extends StreamSource(config) {
    override protected def createStream(ssc: StreamingContext): DStream[Row] = {
      new TestInputDStream(ssc, recordFactory)
    }

    override protected def saveBatchInfo(time: Time): Unit = {}

    override protected def saveDataFrame(df: DataFrame, time: Time): Unit = {
      super.saveDataFrame(df.coalesce(1), time)
    }
  }

}

class StreamSourceSpec extends UnitSpec with BeforeAndAfter {

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

  "StreamSource" should "process field reference" in {
    val tmpDir = File.createTempFile("viyadb-spark-test", null)
    tmpDir.delete()

    try {
      val config = JobConf(
        table = TableConf(
          name = "foo",
          deepStorePath = tmpDir.getAbsolutePath,
          realTime = RealTimeConf(
            parseSpec = Some(ParseSpecConf(
              format = "tsv",
              columns = Some(Seq("company", "dt", "stock_price"))
            )),
            streamSourceClass = Some(classOf[TestStreamSource].getName)
          ),
          batch = BatchConf(),
          dimensions = Seq(
            DimensionConf(name = "company"),
            DimensionConf(name = "dt")
          ),
          metrics = Seq(
            MetricConf(name = "stock_price_sum", field = Some("stock_price"), `type` = "double_sum"),
            MetricConf(name = "stock_price_avg", field = Some("stock_price"), `type` = "double_avg"),
            MetricConf(name = "stock_price_max", field = Some("stock_price"), `type` = "double_max"),
            MetricConf(name = "count", `type` = "count")
          )
        )
      )

      val ssc = new StreamingContext(ss.sparkContext, Seconds(1))

      StreamSource.create(config).start(ssc)

      ssc.start()
      ssc.stop(false, true)
      ssc.awaitTermination()

      val actual = ss.sparkContext.textFile(tmpDir.getAbsolutePath + "/*/*/*/*.gz")
        //.map(row => row.split("\t"))
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