package com.github.viyadb.spark.streaming.record

import java.sql.Timestamp
import java.util.GregorianCalendar

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import com.github.viyadb.spark.streaming.parser.{JsonRecordParser, Record, RecordParser}

class JsonRecordParserSpec extends UnitSpec {

  "JsonRecordParser" should "parse JSON input without field mapping" in {
    val tableConf = TableConf(
      name = "foo",
      dimensions = Seq(
        DimensionConf(name = "app"),
        DimensionConf(name = "date", `type` = Some("time"), format = Some("%Y-%m-%d %H:%M:%S")),
        DimensionConf(name = "network"),
        DimensionConf(name = "city")
      ),
      metrics = Seq(
        MetricConf(name = "revenue", `type` = "double_sum"),
        MetricConf(name = "sessions", `type` = "long_sum")
      )
    )

    val indexerConf = IndexerConf(
      deepStorePath = "",
      realTime = RealTimeConf(
        parseSpec = Some(ParseSpecConf(
          format = "json",
          timeFormats = Some(Map("date" -> "%Y-%m-%d %H:%M:%S"))
        ))
      ),
      batch = BatchConf()
    )

    val jobConf = JobConf(
      indexer = indexerConf,
      tableConfigs = Seq(tableConf)
    )

    val recordParser = RecordParser.create(jobConf)
    assert(recordParser.getClass == classOf[JsonRecordParser])

    val jsonContent = Seq(
      """{
        |  "app": "a.b.c",
        |  "date": "2017-01-01 11:43:55",
        |  "network": "facebook",
        |  "network_id": "123",
        |  "city": "New York",
        |  "sessions": 3,
        |  "revenue": 0.1
        |}""",
      """{
        |  "app": "x.y.z",
        |  "date": "2017-01-03 12:13:00",
        |  "network": "google",
        |  "network_id": "321",
        |  "city": "Boston",
        |  "sessions": 5,
        |  "revenue": 11.1
        |}""",
      """{
        |  "app": "q.w.e",
        |  "date": "2016-12-12 01:20:01",
        |  "network": "facebook",
        |  "network_id": "123",
        |  "city": "San Francisco",
        |  "sessions": 1,
        |  "revenue": 8
        |}"""
    ).map(_.stripMargin)

    val rows = jsonContent.map(json => recordParser.parseRecord("", json).get)
    assert(rows.size == 3)

    assert(rows(0) == new Record(Array("a.b.c", new Timestamp(
      new GregorianCalendar(2017, 0, 1, 11, 43, 55).getTimeInMillis), "facebook", "New York", 0.1, 3L)))

    assert(rows(1) == new Record(Array("x.y.z", new Timestamp(
      new GregorianCalendar(2017, 0, 3, 12, 13, 0).getTimeInMillis), "google", "Boston", 11.1, 5L)))

    assert(rows(2) == new Record(Array("q.w.e", new Timestamp(
      new GregorianCalendar(2016, 11, 12, 1, 20, 1).getTimeInMillis), "facebook", "San Francisco", 8.0, 1L)))
  }

  "JsonRecordParser" should "parse JSON input with field mapping" in {
    val tableConf = TableConf(
      name = "foo",
      dimensions = Seq(
        DimensionConf(name = "app"),
        DimensionConf(name = "date", `type` = Some("time"), format = Some("%Y-%m-%d %H:%M:%S")),
        DimensionConf(name = "network"),
        DimensionConf(name = "city")
      ),
      metrics = Seq(
        MetricConf(name = "revenue", `type` = "double_sum"),
        MetricConf(name = "sessions", `type` = "long_sum")
      )
    )

    val indexerConf = IndexerConf(
      deepStorePath = "",
      realTime = RealTimeConf(
        parseSpec = Some(ParseSpecConf(
          format = "json",
          fieldMapping = Some(Map(
            "app" -> "$.meta.app",
            "date" -> "$.meta.time",
            "network" -> "$.attr.network",
            "city" -> "$.meta.city",
            "revenue" -> "$.stats.revenue",
            "sessions" -> "$.stats.sessions"
          )),
          timeFormats = Some(Map("date" -> "%Y-%m-%d %H:%M:%S"))
        ))
      ),
      batch = BatchConf(
        partitioning = None
      )
    )

    val jobConf = JobConf(
      indexer = indexerConf,
      tableConfigs = Seq(tableConf)
    )

    val recordParser = RecordParser.create(jobConf)
    assert(recordParser.getClass == classOf[JsonRecordParser])

    val jsonContent = Seq(
      """{
        |  "meta": {
        |    "app": "a.b.c",
        |    "city": "New York",
        |    "time": "2017-01-01 11:43:55"
        |  },
        |  "attr": {
        |    "network": "facebook",
        |    "network_id": "123"
        |  },
        |  "stats": {
        |    "sessions": 3,
        |    "revenue": 0.1
        |  }
        |}""",
      """{
        |  "meta": {
        |    "app": "x.y.z",
        |    "city": "Boston",
        |    "time": "2017-01-03 12:13:00"
        |  },
        |  "attr": {
        |    "network": "google",
        |    "network_id": "321"
        |  },
        |  "stats": {
        |    "sessions": 5,
        |    "revenue": 11.1
        |  }
        |}""",
      """{
        |  "meta": {
        |    "app": "q.w.e",
        |    "city": "San Francisco",
        |    "time": "2016-12-12 01:20:01"
        |  },
        |  "attr": {
        |    "network": "facebook",
        |    "network_id": "123"
        |  },
        |  "stats": {
        |    "sessions": 1,
        |    "revenue": 8
        |  }
        |}"""
    ).map(_.stripMargin)

    val rows = jsonContent.map(json => recordParser.parseRecord("", json).get)
    assert(rows.size == 3)

    assert(rows(0) == new Record(Array("a.b.c", new Timestamp(
      new GregorianCalendar(2017, 0, 1, 11, 43, 55).getTimeInMillis), "facebook", "New York", 0.1, 3L)))

    assert(rows(1) == new Record(Array("x.y.z", new Timestamp(
      new GregorianCalendar(2017, 0, 3, 12, 13, 0).getTimeInMillis), "google", "Boston", 11.1, 5L)))

    assert(rows(2) == new Record(Array("q.w.e", new Timestamp(
      new GregorianCalendar(2016, 11, 12, 1, 20, 1).getTimeInMillis), "facebook", "San Francisco", 8.0, 1L)))
  }
}