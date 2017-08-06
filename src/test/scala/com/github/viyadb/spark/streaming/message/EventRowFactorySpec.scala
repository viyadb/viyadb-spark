package com.github.viyadb.spark.streaming.message

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import org.apache.spark.sql.Row

class TestMessageFactory(config: JobConf) extends MessageFactory(config) {
  override def createMessage(meta: String, content: String): Option[Row] = None
}

class EventRowFactorySpec extends UnitSpec {

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