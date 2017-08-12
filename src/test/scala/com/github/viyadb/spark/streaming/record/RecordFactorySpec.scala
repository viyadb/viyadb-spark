package com.github.viyadb.spark.streaming.record

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import com.github.viyadb.spark.streaming.record.RecordFactorySpec.TestRecordFactory

object RecordFactorySpec {

  class TestRecordFactory(config: JobConf) extends RecordFactory(config) {
  }

}

class RecordFactorySpec extends UnitSpec {

  "RecordFactory" should "support recordFactoryClass" in {
    val config = JobConf(
      table = TableConf(
        name = "foo",
        deepStorePath = "",
        realTime = RealTimeConf(
          recordFactoryClass = Some(classOf[TestRecordFactory].getName)
        ),
        batch = BatchConf(),
        dimensions = Seq(),
        metrics = Seq()
      )
    )

    val recordFactory = RecordFactory.create(config)
    assert(recordFactory.getClass == classOf[TestRecordFactory])
  }
}