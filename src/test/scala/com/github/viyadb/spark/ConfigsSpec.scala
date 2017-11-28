package com.github.viyadb.spark

import com.github.viyadb.spark.Configs.IndexerConf

class ConfigsSpec extends UnitSpec {

  "IndexerConf" should "parse" in {
    val json =
      """
       |{
       |  "deepStorePath": "",
       |  "realTime": {
       |    "windowDuration": "PT1M",
       |    "parseSpec": {
       |      "format": "tsv",
       |      "columns": ["first", "second"]
       |    }
       |  },
       |  "batch": {
       |    "keepInterval": "2017-01-01/P1M",
       |    "partitioning": {
       |      "column": "app_id",
       |      "numPartitions": 1
       |    }
       |  },
       |  "tables": ["foo"]
       |}
      """.stripMargin

    val config = Configs.parseConf[IndexerConf](json)

    assert(config.realTime.windowDuration.get.toStandardSeconds.getSeconds == 60)
    assert(config.batch.keepInterval.get.getEnd.toString("yyyy-MM-dd") == "2017-02-01")
    assert(config.realTime.parseSpec.get.columns.get.size == 2)
    assert(config.batch.partitioning.get.column == "app_id")
  }
}