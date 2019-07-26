package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.{JobConf, TableConf}
import com.github.viyadb.spark.streaming.parser.{Record, RecordParser}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

object StreamingTestUtils {

  class TestRDD(@transient val _sc: SparkContext, val recordParser: RecordParser)
    extends RDD[Record](_sc, Nil) {

    override def compute(split: Partition, context: TaskContext): Iterator[Record] = {
      Seq(
        Seq("IBM", "2015-01-01", "101.1").mkString("\t"),
        Seq("IBM", "2015-01-01", "102.32").mkString("\t"),
        Seq("IBM", "2015-01-02", "105.0").mkString("\t"),
        Seq("IBM", "2015-01-02", "99.7").mkString("\t"),
        Seq("IBM", "2015-01-03", "98.12").mkString("\t"),
        Seq("Amdocs", "2015-01-01", "50.0").mkString("\t"),
        Seq("Amdocs", "2015-01-01", "57.14").mkString("\t"),
        Seq("Amdocs", "2015-01-02", "89.22").mkString("\t"),
        Seq("Amdocs", "2015-01-02", "90.3").mkString("\t"),
        Seq("Amdocs", "2015-01-03", "1.01").mkString("\t")
      )
        .map(recordParser.parseRecord("", _).get).toIterator
    }

    override protected def getPartitions: Array[Partition] = {
      Array(new Partition {
        override def index: Int = 0
      })
    }
  }

  class TestInputDStream(ssc_ : StreamingContext, val recordParser: RecordParser)
    extends InputDStream[Record](ssc_) {

    var sent = false

    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def compute(validTime: Time): Option[RDD[Record]] = {
      if (sent) {
        None
      } else {
        sent = true
        Some(new TestRDD(ssc_.sparkContext, recordParser))
      }
    }
  }

  class TestStreamingProcess(jobConf: JobConf) extends StreamingProcess(jobConf) {
    override protected def createStream(ssc: StreamingContext): DStream[Record] = {
      new TestInputDStream(ssc, recordParser)
    }

    override protected def saveDataFrame(tableConf: TableConf, df: DataFrame, time: Time): Unit = {
      super.saveDataFrame(tableConf, df.coalesce(1), time)
    }
  }
}
