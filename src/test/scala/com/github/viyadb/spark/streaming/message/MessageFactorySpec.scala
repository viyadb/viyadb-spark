package com.github.viyadb.spark.streaming.message

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec

class TestMessageFactory(config: JobConf) extends MessageFactory(config) {
}

class MessageFactorySpec extends UnitSpec {

  "MessageFactory" should "support messageFactoryClass" in {
    val config = JobConf(
      table = TableConf(
        name = "foo",
        realTime = RealTimeConf(
          outputPath = "",
          messageFactoryClass = Some(classOf[TestMessageFactory].getName)
        ),
        batch = BatchConf(),
        dimensions = Seq(),
        metrics = Seq()
      )
    )

    val messageFactory = MessageFactory.create(config)
    assert(messageFactory.getClass == classOf[TestMessageFactory])
  }
}