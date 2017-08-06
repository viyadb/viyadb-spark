package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import org.apache.spark.sql.DataFrame

class TestProcessor(config: JobConf) extends Processor(config) {
  override def process(df: DataFrame): DataFrame = df
}

class ProcessorSpec extends UnitSpec {

  "Processor" should "support processorClass" in {
    val config = JobConf(
      table = TableConf(
        name = "foo",
        realTime = RealTimeConf(
          outputPath = ""
        ),
        batch = BatchConf(),
        processorClass = Some(classOf[TestProcessor].getName),
        dimensions = Seq(),
        metrics = Seq()
      )
    )

    val processor = Processor.create(config)
    assert(processor.nonEmpty)
    assert(processor.get.getClass == classOf[TestProcessor])
  }
}