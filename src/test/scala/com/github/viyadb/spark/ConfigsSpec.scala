package com.github.viyadb.spark

import com.github.viyadb.spark.Configs._

class ConfigsSpec extends UnitSpec {

  "TableConfig" should "parse" in {
    val json =
      """
       |{
       |  "name": "foo",
       |  "realTime": {
       |    "windowDuration": "PT1M",
       |    "parseSpec": {
       |      "format": "tsv",
       |      "columns": ["first", "second"]
       |    },
       |    "outputPath": ""
       |  },
       |  "batch": {
       |    "keepInterval": "2017-01-01/P1M"
       |  }
       |}
      """.stripMargin

    val config = parseTableConf(json)

    assert(config.realTime.windowDuration.get.toStandardSeconds.getSeconds == 60)
    assert(config.batch.keepInterval.get.getEnd.toString("yyyy-MM-dd") == "2017-02-01")
    assert(config.realTime.parseSpec.get.columns.get.size == 2)
  }
}