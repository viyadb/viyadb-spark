package com.github.viyadb.spark.streaming.record

import java.sql.Timestamp
import java.util.GregorianCalendar

import com.github.viyadb.spark.Configs.{DimensionConf, _}
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
        |}""",
      """{ broken JSON shouldn't make the process fail
        |  "meta: {
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

    val rows = jsonContent.map(json => recordParser.parseRecord("", json)).filter(_.nonEmpty).map(_.get)
    assert(rows.size == 3)

    assert(rows(0) == new Record(Array("a.b.c", new Timestamp(
      new GregorianCalendar(2017, 0, 1, 11, 43, 55).getTimeInMillis), "facebook", "New York", 0.1, 3L)))

    assert(rows(1) == new Record(Array("x.y.z", new Timestamp(
      new GregorianCalendar(2017, 0, 3, 12, 13, 0).getTimeInMillis), "google", "Boston", 11.1, 5L)))

    assert(rows(2) == new Record(Array("q.w.e", new Timestamp(
      new GregorianCalendar(2016, 11, 12, 1, 20, 1).getTimeInMillis), "facebook", "San Francisco", 8.0, 1L)))
  }

  "JsonRecordParser" should "parse JSON with different types" in {
    val tableConf = TableConf(
      name = "foo",
      dimensions = Seq(
        DimensionConf(name = "string", `type` = Some("string")),
        DimensionConf(name = "numeric", `type` = Some("numeric")),
        DimensionConf(name = "time", `type` = Some("time"), format = Some("%Y-%m-%dT%H:%M:%S%z")),
        DimensionConf(name = "microtime", `type` = Some("microtime"), format = Some("%Y-%m-%dT%H:%M:%S.%f%z")),
        DimensionConf(name = "byte", `type` = Some("byte")),
        DimensionConf(name = "ubyte", `type` = Some("ubyte")),
        DimensionConf(name = "short", `type` = Some("short")),
        DimensionConf(name = "ushort", `type` = Some("ushort")),
        DimensionConf(name = "int", `type` = Some("int")),
        DimensionConf(name = "uint", `type` = Some("uint")),
        DimensionConf(name = "long", `type` = Some("long")),
        DimensionConf(name = "ulong", `type` = Some("ulong")),
        DimensionConf(name = "float", `type` = Some("float")),
        DimensionConf(name = "double", `type` = Some("double"))
      ),
      metrics = Seq(
        MetricConf(name = "count", `type` = "count"),
        MetricConf(name = "byte_sum", `type` = "byte_sum"),
        MetricConf(name = "ubyte_sum", `type` = "ubyte_sum"),
        MetricConf(name = "short_sum", `type` = "short_sum"),
        MetricConf(name = "ushort_sum", `type` = "ushort_sum"),
        MetricConf(name = "int_sum", `type` = "int_sum"),
        MetricConf(name = "uint_sum", `type` = "uint_sum"),
        MetricConf(name = "long_sum", `type` = "long_sum"),
        MetricConf(name = "ulong_sum", `type` = "ulong_sum"),
        MetricConf(name = "float_sum", `type` = "float_sum"),
        MetricConf(name = "double_sum", `type` = "double_sum")
      )
    )

    val indexerConf = IndexerConf(
      deepStorePath = "",
      realTime = RealTimeConf(
        parseSpec = Some(ParseSpecConf(
          format = "json"
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

    val columns = Seq("string", "numeric", "time", "microtime", "byte", "ubyte", "short",
      "ushort", "int", "uint", "long", "ulong", "float", "double", "byte_sum", "ubyte_sum",
      "short_sum", "ushort_sum", "int_sum", "uint_sum", "long_sum", "ulong_sum", "float_sum",
      "double_sum")

    val jsonContent = Seq(
      Seq("A", 123, "2019-01-05T01:02:03+0000", "2019-01-05T01:02:03.123+0000",
        -0xa, 0xa, 5, 5, -120, 120, -123456, 123456, 1.23456F, 1.23456,
        -0xa, 0xa, 5, 5, -120, 120, -123456, 123456, 1.23456F, 1.23456)
    ).map { values =>
      "{" +
        columns.zip(values).map { case (column, value) =>
          value match {
            case s: String => s""""$column":"$value""""
            case _ => s""""$column":$value"""
          }
        }.mkString(",") + "}"
    }

    val actual = jsonContent.map(json => recordParser.parseRecord("", json).get)
    val expected = Seq(
      new Record(Array("A", 123, new Timestamp(1546650123000L), new Timestamp(1546650123123L),
        -0xa, 0xa, 5, 5, -120, 120, -123456, 123456, 1.23456F, 1.23456,
        -0xa, 0xa, 5, 5, -120, 120, -123456, 123456, 1.23456F, 1.23456))
    )
    assert(actual == expected)

    val expectedTypes = Seq(
      classOf[java.lang.String], classOf[java.lang.Integer], classOf[java.sql.Timestamp],
      classOf[java.sql.Timestamp], classOf[java.lang.Byte], classOf[java.lang.Short],
      classOf[java.lang.Short], classOf[java.lang.Integer], classOf[java.lang.Integer],
      classOf[java.lang.Long], classOf[java.lang.Long], classOf[java.lang.Long],
      classOf[java.lang.Float], classOf[java.lang.Double], classOf[java.lang.Byte],
      classOf[java.lang.Short], classOf[java.lang.Short], classOf[java.lang.Integer],
      classOf[java.lang.Integer], classOf[java.lang.Long], classOf[java.lang.Long],
      classOf[java.lang.Long], classOf[java.lang.Float], classOf[java.lang.Double]
    )
    assert(actual.map(r => r.values.map(v => v.getClass).toSeq).head == expectedTypes)
  }
}