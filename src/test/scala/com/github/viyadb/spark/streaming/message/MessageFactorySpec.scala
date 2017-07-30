package com.github.viyadb.spark.streaming.message

import com.github.viyadb.spark.TableConfig._
import com.github.viyadb.spark.UnitSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class TestMessageFactory(table: Table) extends MessageFactory(table) {
  override def createDataFrame(rdd: RDD[AnyRef]): DataFrame = null
}

class MessageFactorySpec extends UnitSpec {

  "MessageFactory" should "support messageFactoryClass" in {
    val tableConf = Table(
      realTime = RealTime(
        parseSpec = None,
        kafka = None,
        outputPath = "",
        messageFactoryClass = Some(classOf[TestMessageFactory].getName)
      ),
      batch = Batch(keepInterval = None),
      processorClass = None,
      dimensions = Seq(),
      metrics = Seq()
    )

    val messageFactory = MessageFactory.create(tableConf)
    assert(messageFactory.getClass == classOf[TestMessageFactory])
  }
}