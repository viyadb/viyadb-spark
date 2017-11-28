package com.github.viyadb.spark.streaming.record

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import com.github.viyadb.spark.streaming.parser.{Record, RecordParser}
import com.github.viyadb.spark.streaming.record.RecordParserSpec.TestRecordParser

object RecordParserSpec {

  class TestRecordParser(jobConf: JobConf) extends RecordParser(jobConf) {
    override def parseRecord(topic: String, record: String): Option[Record] = {
      None
    }
  }

}

class RecordParserSpec extends UnitSpec {

  "RecordParser" should "support recordFactoryClass" in {
    val indexerConf = IndexerConf(
      deepStorePath = "",
      realTime = RealTimeConf(
        parseSpec = Some(ParseSpecConf(
          recordParserClass = Some(classOf[TestRecordParser].getName)
        ))
      ),
      batch = BatchConf()
    )

    val jobConf = JobConf(
      indexer = indexerConf,
      tableConfigs = Seq()
    )

    val recordParser = RecordParser.create(jobConf)
    assert(recordParser.getClass == classOf[TestRecordParser])
  }
}