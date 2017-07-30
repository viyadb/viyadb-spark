package com.github.viyadb.spark

import com.github.viyadb.spark.TableConfig._

class TableConfigSpec extends UnitSpec {

  "TableConfig" should "parse" in {
    val json =
      """
       |{
       |  "realTime": {
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

    val table = parse(json)

    assert(table.batch.keepInterval.get.getEnd.toString("yyyy-MM-dd") == "2017-02-01")

    assert(table.realTime.parseSpec.get.columns.get.size == 2)
  }
}