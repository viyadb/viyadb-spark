package com.github.viyadb.spark.batch

import java.util.TimeZone

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfter

class BatchProcessorSpec extends UnitSpec with BeforeAndAfter {

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

  "BatchProcessor" should "process all metric types" in {
    val config = JobConf(
      table = TableConf(
        name = "foo",
        deepStorePath = "",
        realTime = RealTimeConf(
          parseSpec = Some(ParseSpecConf(
            format = "tsv"
          ))
        ),
        batch = BatchConf(),
        dimensions = Seq(
          DimensionConf(name = "country")
        ),
        metrics = Seq(
          MetricConf(name = "byte_sum", `type` = "byte_sum"),
          MetricConf(name = "byte_max", `type` = "byte_max"),
          MetricConf(name = "byte_min", `type` = "byte_min"),
          MetricConf(name = "byte_avg", `type` = "byte_avg"),
          MetricConf(name = "ubyte_sum", `type` = "ubyte_sum"),
          MetricConf(name = "ubyte_max", `type` = "ubyte_max"),
          MetricConf(name = "ubyte_min", `type` = "ubyte_min"),
          MetricConf(name = "ubyte_avg", `type` = "ubyte_avg"),
          MetricConf(name = "short_sum", `type` = "short_sum"),
          MetricConf(name = "short_max", `type` = "short_max"),
          MetricConf(name = "short_min", `type` = "short_min"),
          MetricConf(name = "short_avg", `type` = "short_avg"),
          MetricConf(name = "ushort_sum", `type` = "ushort_sum"),
          MetricConf(name = "ushort_max", `type` = "ushort_max"),
          MetricConf(name = "ushort_min", `type` = "ushort_min"),
          MetricConf(name = "ushort_avg", `type` = "ushort_avg"),
          MetricConf(name = "int_sum", `type` = "int_sum"),
          MetricConf(name = "int_max", `type` = "int_max"),
          MetricConf(name = "int_min", `type` = "int_min"),
          MetricConf(name = "int_avg", `type` = "int_avg"),
          MetricConf(name = "uint_sum", `type` = "uint_sum"),
          MetricConf(name = "uint_max", `type` = "uint_max"),
          MetricConf(name = "uint_min", `type` = "uint_min"),
          MetricConf(name = "uint_avg", `type` = "uint_avg"),
          MetricConf(name = "long_sum", `type` = "long_sum"),
          MetricConf(name = "long_max", `type` = "long_max"),
          MetricConf(name = "long_min", `type` = "long_min"),
          MetricConf(name = "long_avg", `type` = "long_avg"),
          MetricConf(name = "ulong_sum", `type` = "ulong_sum"),
          MetricConf(name = "ulong_max", `type` = "ulong_max"),
          MetricConf(name = "ulong_min", `type` = "ulong_min"),
          MetricConf(name = "ulong_avg", `type` = "ulong_avg"),
          MetricConf(name = "float_sum", `type` = "float_sum"),
          MetricConf(name = "float_max", `type` = "float_max"),
          MetricConf(name = "float_min", `type` = "float_min"),
          MetricConf(name = "float_avg", `type` = "float_avg"),
          MetricConf(name = "double_sum", `type` = "double_sum"),
          MetricConf(name = "double_max", `type` = "double_max"),
          MetricConf(name = "double_min", `type` = "double_min"),
          MetricConf(name = "double_avg", `type` = "double_avg")
        )
      )
    )

    val tsvContent = Seq(
      Array("US") ++ List.tabulate(40)(_ => "1") :+ "1",
      Array("US") ++ List.tabulate(40)(_ => "2") :+ "1",
      Array("US") ++ List.tabulate(40)(_ => "3") :+ "1",
      Array("IL") ++ List.tabulate(40)(_ => "1") :+ "1",
      Array("IL") ++ List.tabulate(40)(_ => "2") :+ "1"
    )
    val loader = new MicroBatchLoader(config)
    val records = loader.createDataFrame(ss.sparkContext.makeRDD[Row](tsvContent.map(loader.parseInputRow(_))))

    val batchProcessor = new BatchProcessor(config)
    val processed = batchProcessor.process(records)
    val actual = processed.rdd.map(row => row.toSeq).collect().toSet

    val expected = Set(
      Seq("US") ++ List.tabulate(10)(_ => Seq(6, 3, 1, 6)).flatten :+ 3,
      Seq("IL") ++ List.tabulate(10)(_ => Seq(3, 2, 1, 3)).flatten :+ 2
    )

    assert(actual == expected)
  }

  "StreamingProcessor" should "process all metric types with count" in {
    val config = JobConf(
      table = TableConf(
        name = "foo",
        deepStorePath = "",
        realTime = RealTimeConf(
          parseSpec = Some(ParseSpecConf(
            format = "tsv"
          ))
        ),
        batch = BatchConf(),
        dimensions = Seq(
          DimensionConf(name = "country")
        ),
        metrics = Seq(
          MetricConf(name = "count", `type` = "count"),
          MetricConf(name = "byte_sum", `type` = "byte_sum"),
          MetricConf(name = "byte_max", `type` = "byte_max"),
          MetricConf(name = "byte_min", `type` = "byte_min"),
          MetricConf(name = "byte_avg", `type` = "byte_avg"),
          MetricConf(name = "ubyte_sum", `type` = "ubyte_sum"),
          MetricConf(name = "ubyte_max", `type` = "ubyte_max"),
          MetricConf(name = "ubyte_min", `type` = "ubyte_min"),
          MetricConf(name = "ubyte_avg", `type` = "ubyte_avg"),
          MetricConf(name = "short_sum", `type` = "short_sum"),
          MetricConf(name = "short_max", `type` = "short_max"),
          MetricConf(name = "short_min", `type` = "short_min"),
          MetricConf(name = "short_avg", `type` = "short_avg"),
          MetricConf(name = "ushort_sum", `type` = "ushort_sum"),
          MetricConf(name = "ushort_max", `type` = "ushort_max"),
          MetricConf(name = "ushort_min", `type` = "ushort_min"),
          MetricConf(name = "ushort_avg", `type` = "ushort_avg"),
          MetricConf(name = "int_sum", `type` = "int_sum"),
          MetricConf(name = "int_max", `type` = "int_max"),
          MetricConf(name = "int_min", `type` = "int_min"),
          MetricConf(name = "int_avg", `type` = "int_avg"),
          MetricConf(name = "uint_sum", `type` = "uint_sum"),
          MetricConf(name = "uint_max", `type` = "uint_max"),
          MetricConf(name = "uint_min", `type` = "uint_min"),
          MetricConf(name = "uint_avg", `type` = "uint_avg"),
          MetricConf(name = "long_sum", `type` = "long_sum"),
          MetricConf(name = "long_max", `type` = "long_max"),
          MetricConf(name = "long_min", `type` = "long_min"),
          MetricConf(name = "long_avg", `type` = "long_avg"),
          MetricConf(name = "ulong_sum", `type` = "ulong_sum"),
          MetricConf(name = "ulong_max", `type` = "ulong_max"),
          MetricConf(name = "ulong_min", `type` = "ulong_min"),
          MetricConf(name = "ulong_avg", `type` = "ulong_avg"),
          MetricConf(name = "float_sum", `type` = "float_sum"),
          MetricConf(name = "float_max", `type` = "float_max"),
          MetricConf(name = "float_min", `type` = "float_min"),
          MetricConf(name = "float_avg", `type` = "float_avg"),
          MetricConf(name = "double_sum", `type` = "double_sum"),
          MetricConf(name = "double_max", `type` = "double_max"),
          MetricConf(name = "double_min", `type` = "double_min"),
          MetricConf(name = "double_avg", `type` = "double_avg")
        )
      )
    )

    val tsvContent = Seq(
      Array("US", "1") ++ List.tabulate(40)(_ => "1"),
      Array("US", "1") ++ List.tabulate(40)(_ => "2"),
      Array("US", "1") ++ List.tabulate(40)(_ => "3"),
      Array("IL", "1") ++ List.tabulate(40)(_ => "1"),
      Array("IL", "1") ++ List.tabulate(40)(_ => "2")
    )
    val loader = new MicroBatchLoader(config)
    val records = loader.createDataFrame(ss.sparkContext.makeRDD[Row](tsvContent.map(loader.parseInputRow(_))))

    val batchProcessor = new BatchProcessor(config)
    val processed = batchProcessor.process(records)
    val actual = processed.rdd.map(row => row.toSeq).collect().toSet

    val expected = Set(
      Seq("US", 3) ++ List.tabulate(10)(_ => Seq(6, 3, 1, 6)).flatten,
      Seq("IL", 2) ++ List.tabulate(10)(_ => Seq(3, 2, 1, 3)).flatten
    )

    assert(actual == expected)
  }

  "StreamingProcessor" should "support field reference" in {
    val config = JobConf(
      table = TableConf(
        name = "foo",
        deepStorePath = "",
        realTime = RealTimeConf(
          parseSpec = Some(ParseSpecConf(
            format = "tsv",
            columns = Some(Seq("company", "dt", "stock_price"))
          ))
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

    val tsvContent = Seq(
      Array("IBM", "2015-01-01", "101.1", "101.1", "101.1", "1"),
      Array("IBM", "2015-01-01", "102.32", "102.32", "102.32", "1"),
      Array("IBM", "2015-01-02", "105.0", "105.0", "105.0", "1"),
      Array("IBM", "2015-01-02", "99.7", "99.7", "99.7", "1"),
      Array("IBM", "2015-01-03", "98.12", "98.12", "98.12", "1"),
      Array("Amdocs", "2015-01-01", "50.0", "50.0", "50.0", "1"),
      Array("Amdocs", "2015-01-01", "57.14", "57.14", "57.14", "1"),
      Array("Amdocs", "2015-01-02", "89.22", "89.22", "89.22", "1"),
      Array("Amdocs", "2015-01-02", "90.3", "90.3", "90.3", "1"),
      Array("Amdocs", "2015-01-03", "1.01", "1.01", "1.01", "1")
    )
    val loader = new MicroBatchLoader(config)
    val records = loader.createDataFrame(ss.sparkContext.makeRDD[Row](tsvContent.map(loader.parseInputRow(_))))

    val batchProcessor = new BatchProcessor(config)
    val processed = batchProcessor.process(records)
    val actual = processed.rdd.map(row => row.toSeq).collect().toSet

    val expected = Set(
      Seq("Amdocs", "2015-01-01", 107.14, 107.14, 57.14, 2),
      Seq("IBM", "2015-01-02", 204.7, 204.7, 105.0, 2),
      Seq("IBM", "2015-01-03", 98.12, 98.12, 98.12, 1),
      Seq("Amdocs", "2015-01-03", 1.01, 1.01, 1.01, 1),
      Seq("Amdocs", "2015-01-02", 179.51999999999998, 179.51999999999998, 90.3, 2),
      Seq("IBM", "2015-01-01", 203.42, 203.42, 102.32, 2)
    )

    assert(actual == expected)
  }
}