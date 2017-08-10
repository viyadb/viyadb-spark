package com.github.viyadb.spark.streaming.message

import java.sql.Timestamp
import java.util.GregorianCalendar

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec

class TsvMessageFactorySpec extends UnitSpec {

  "TsvMessageFactory" should "parse TSV input" in {
    val config = JobConf(
      table = TableConf(
        name = "foo",
        deepStorePath = "",
        realTime = RealTimeConf(
          parseSpec = Some(ParseSpecConf(
            format = "tsv",
            columns = Some(Seq("app", "date", "network", "network_id", "city", "sessions", "installs", "revenue"))
          ))
        ),
        batch = BatchConf(),
        dimensions = Seq(
          DimensionConf(name = "app"),
          DimensionConf(name = "date", `type` = Some("time"), format = Some("%Y-%m-%d %H:%M:%S")),
          DimensionConf(name = "network"),
          DimensionConf(name = "city")
        ),
        metrics = Seq(
          MetricConf(name = "revenue", `type` = "double_sum"),
          MetricConf(name = "sessions", `type` = "long_sum"),
          MetricConf(name = "installs", `type` = "int_sum")
        )
      )
    )

    val messageFactory = MessageFactory.create(config)
    assert(messageFactory.getClass == classOf[TsvMessageFactory])

    val tsvContent = Seq(
      "a.b.c\t2017-01-01 11:43:55\tfacebook\t123\tNew York\t30\t4\t0.1",
      "x.y.z\t2017-01-03 12:13:00\tgoogle\t321\tBoston\t50\t5\t11.1",
      "q.w.e\t2016-12-12 01:20:01\tfacebook\t123\tSan Francisco\t10\t6\t8"
    )

    val rows = tsvContent.map(tsv => messageFactory.createMessage("", tsv).get)
    assert(rows.size == 3)

    assert(rows(0) == new Message(Array("a.b.c", new Timestamp(
      new GregorianCalendar(2017, 0, 1, 11, 43, 55).getTimeInMillis), "facebook", "New York", 30L, 4, 0.1)))

    assert(rows(1) == new Message(Array("x.y.z", new Timestamp(
      new GregorianCalendar(2017, 0, 3, 12, 13, 0).getTimeInMillis), "google", "Boston", 50L, 5, 11.1)))

    assert(rows(2) == new Message(Array("q.w.e", new Timestamp(
      new GregorianCalendar(2016, 11, 12, 1, 20, 1).getTimeInMillis), "facebook", "San Francisco", 10L, 6, 8.0)))
  }
}