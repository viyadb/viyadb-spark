package com.github.viyadb.spark.processing

import com.github.viyadb.spark.TableConfig._
import com.github.viyadb.spark.UnitSpec
import org.apache.spark.sql.DataFrame

class TestProcessor(table: Table) extends Processor(table) {
  override def process(df: DataFrame): DataFrame = df
}

class ProcessorSpec extends UnitSpec {

  "Processor" should "support processorClass" in {
    val tableConf = Table(
      realTime = RealTime(
        parseSpec = None,
        kafka = None,
        outputPath = "",
        messageFactoryClass = None
      ),
      batch = Batch(keepInterval = None),
      processorClass = Some(classOf[TestProcessor].getName),
      dimensions = Seq(),
      metrics = Seq()
    )

    val processor = Processor.create(tableConf)
    assert(processor.getClass == classOf[TestProcessor])
  }
}