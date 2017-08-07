package com.github.viyadb.spark.processing

import java.sql.Timestamp

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter

case class TimeEvent(y: Timestamp, m: Timestamp, d: Timestamp, h: Timestamp, min: Timestamp, s: Timestamp)

class TimeTruncatorSpec extends UnitSpec with BeforeAndAfter {

  private var ss: SparkSession = _

  before {
    ss = SparkSession.builder().appName(getClass.getName)
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (ss != null) {
      ss.stop()
    }
  }

  "Rolluper" should "truncate dimensions" in {
    val config = JobConf(
      table = TableConf(
        name = "",
        realTime = RealTimeConf(
          outputPath = ""
        ),
        batch = BatchConf(),
        dimensions = Seq(
          DimensionConf(name = "y", `type` = Some("time"), granularity = Some("year")),
          DimensionConf(name = "m", `type` = Some("time"), granularity = Some("month")),
          DimensionConf(name = "d", `type` = Some("time"), granularity = Some("day")),
          DimensionConf(name = "h", `type` = Some("time"), granularity = Some("hour")),
          DimensionConf(name = "min", `type` = Some("time"), granularity = Some("minute")),
          DimensionConf(name = "s", `type` = Some("time"), granularity = Some("second"))
        ),
        metrics = Seq()
      )
    )

    val sparkSession = ss
    import sparkSession.implicits._

    val df = ss.createDataset(
      Array(
        1502110766000L,
        1331164754000L,
        1388534461000L
      )
        .map(t => TimeEvent(new Timestamp(t), new Timestamp(t), new Timestamp(t),
          new Timestamp(t), new Timestamp(t), new Timestamp(t)))
    ).toDF()

    val result = new TimeTruncator(config).process(df)
      .map(row => row.toSeq.map(_.toString)).collect()

    assert(result(0) == Seq("2017-01-01", "2017-08-01", "2017-08-07", "2017-08-07 15:00:00.0",
      "2017-08-07 15:59:00.0", "2017-08-07 15:59:26.0"))

    assert(result(1) == Seq("2012-01-01", "2012-03-01", "2012-03-08", "2012-03-08 01:00:00.0",
      "2012-03-08 01:59:00.0", "2012-03-08 01:59:14.0"))

    assert(result(2) == Seq("2014-01-01", "2014-01-01", "2014-01-01", "2014-01-01 02:00:00.0",
      "2014-01-01 02:01:00.0", "2014-01-01 02:01:01.0"))
  }
}