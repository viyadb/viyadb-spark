package com.github.viyadb.spark.streaming.kafka

import java.io.File
import java.time.Duration
import java.util.{Properties, TimeZone}

import com.github.viyadb.spark.Configs._
import com.github.viyadb.spark.UnitSpec
import com.github.viyadb.spark.notifications.Notifier
import com.github.viyadb.spark.streaming.{KafkaStreamingProcess, StreamingProcess}
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.Period
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.wait.strategy.Wait

import scala.collection.JavaConversions._
import scala.util.Random

class KafkaStreamingProcessSpec extends UnitSpec with BeforeAndAfterAll with BeforeAndAfter {

  private var ss: SparkSession = _
  private var kafka: KafkaContainer = _
  private var kafkaBrokers: String = _
  private var producer: KafkaProducer[String, String] = _
  private var consumer: KafkaConsumer[String, String] = _

  override def beforeAll() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    kafka = new KafkaContainer()
    kafka.start()

    kafkaBrokers = kafka.getBootstrapServers.replace("PLAINTEXT://", "")
  }

  override def afterAll(): Unit = {
    if (kafka != null) {
      kafka.stop()
    }
  }

  before {
    val producerConf = new Properties()
    producerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    producerConf.put(ProducerConfig.ACKS_CONFIG, "all")
    producerConf.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Integer.MAX_VALUE.toString)
    producerConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producer = new KafkaProducer[String, String](producerConf)

    val consumerConf = new Properties()
    consumerConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    consumerConf.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-streaming-test")
    consumerConf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerConf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerConf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)
    consumer = new KafkaConsumer[String, String](consumerConf)

    ss = SparkSession.builder().appName(getClass.getName)
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (ss != null) {
      ss.stop()
    }
    if (producer != null) {
      producer.close()
    }
    if (consumer != null) {
      consumer.close()
    }
  }

  def sendEventsToKafka(topic: String, events: Seq[String]): Unit = {
    events.foreach { event =>
      producer.send(
        new ProducerRecord[String, String](topic, null, event)).get()
    }
  }

  "KafkaStreamingProcessor" should "process field reference" in {
    val tmpDir = File.createTempFile("viyadb-spark-test", null)
    tmpDir.delete()

    try {
      val suffix = Math.abs(Random.nextInt())
      val tableConf = TableConf(
        name = s"events$suffix",
        dimensions = Seq(
          DimensionConf(name = "company"),
          DimensionConf(name = "timestamp", `type` = Some("time"), format = Some("%Y-%m-%d"))
        ),
        metrics = Seq(
          MetricConf(name = "stock_price_sum", field = Some("stock_price"), `type` = "double_sum"),
          MetricConf(name = "stock_price_avg", field = Some("stock_price"), `type` = "double_avg"),
          MetricConf(name = "stock_price_max", field = Some("stock_price"), `type` = "double_max"),
          MetricConf(name = "count", `type` = "count")
        )
      )

      val indexerConf = IndexerConf(
        deepStorePath = tmpDir.getAbsolutePath,
        realTime = RealTimeConf(
          windowDuration = Some(Period.seconds(10)),
          kafkaSource = Some(KafkaSourceConf(
            topics = Seq(s"events$suffix"),
            brokers = Seq(kafkaBrokers)
          )),
          parseSpec = Some(ParseSpecConf(
            format = "json",
            columns = Some(Seq("company", "timestamp", "stock_price")),
            timeColumn = Some("timestamp"),
            timeFormats = Some(Map("timestamp" -> "%Y-%m-%d"))
          )),
          streamingProcessClass = Some(classOf[KafkaStreamingProcess].getName),
          notifier = NotifierConf(
            `type` = "kafka",
            channel = kafkaBrokers,
            queue = s"notifications$suffix"
          )
        ),
        batch = BatchConf()
      )

      val jobConf = JobConf(
        indexer = indexerConf,
        tableConfigs = Seq(tableConf)
      )

      val ssc = new StreamingContext(ss.sparkContext, Seconds(1))

      StreamingProcess.create(jobConf).start(ssc)
      ssc.start()

      val events = Seq(
        ("IBM", "2015-01-01", "101.1"),
        ("IBM", "2015-01-01", "102.32"),
        ("IBM", "2015-01-02", "105.0"),
        ("IBM", "2015-01-02", "99.7"),
        ("IBM", "2015-01-03", "98.12"),
        ("Amdocs", "2015-01-01", "50.0"),
        ("Amdocs", "2015-01-01", "57.14"),
        ("Amdocs", "2015-01-02", "89.22"),
        ("Amdocs", "2015-01-02", "90.3"),
        ("Amdocs", "2015-01-03", "1.01")
      ).map { event =>
        s"""{
           |"company": "${event._1}","timestamp": "${event._2}","stock_price": "${event._3}"
           |}""".stripMargin
      }

      sendEventsToKafka(s"events$suffix", events)

      Thread.sleep(10000L)
      ssc.stop(stopSparkContext = false, stopGracefully = true)
      ssc.awaitTermination()

      val actual = ss.sparkContext.textFile(s"${jobConf.indexer.realtimePrefix}/events$suffix/*/*/*.gz")
        .collect().sorted.mkString("\n")

      val expected = Array(
        Array("Amdocs", "2015-01-01", "107.14", "107.14", "57.14", "2").mkString("\t"),
        Array("IBM", "2015-01-02", "204.7", "204.7", "105.0", "2").mkString("\t"),
        Array("IBM", "2015-01-03", "98.12", "98.12", "98.12", "1").mkString("\t"),
        Array("Amdocs", "2015-01-03", "1.01", "1.01", "1.01", "1").mkString("\t"),
        Array("Amdocs", "2015-01-02", "179.51999999999998", "179.51999999999998", "90.3", "2").mkString("\t"),
        Array("IBM", "2015-01-01", "203.42", "203.42", "102.32", "2").mkString("\t")
      )
        .sorted.mkString("\n")

      assert(actual == expected)

      val partitions = Seq(new TopicPartition(s"notifications$suffix", 0))
      consumer.assign(partitions)
      consumer.seekToBeginning(partitions)
      val records = consumer.poll(Duration.ofSeconds(1))
      assert(records.count() == 1)

    } finally {
      FileUtils.deleteDirectory(tmpDir)
    }
  }

  "KafkaNotifier" should "support sending and receiving messages" in {
    val suffix = Math.abs(Random.nextInt())
    val indexerConf = IndexerConf(
      deepStorePath = "",
      realTime = RealTimeConf(
        notifier = NotifierConf(
          `type` = "kafka",
          channel = kafkaBrokers,
          queue = s"notifications$suffix"
        )
      ),
      batch = BatchConf()
    )
    val jobConf = JobConf(
      indexer = indexerConf,
      tableConfigs = Seq()
    )

    val kafkaNotifier = Notifier.create[String](jobConf, indexerConf.realTime.notifier)
    kafkaNotifier.send(1, "hello")
    kafkaNotifier.send(2, "world")

    assert(kafkaNotifier.lastMessage.get == "world")
    assert(kafkaNotifier.allMessages == Seq("hello", "world"))

    kafkaNotifier.send(3, "again")
    assert(kafkaNotifier.lastMessage.get == "again")
    assert(kafkaNotifier.allMessages == Seq("hello", "world", "again"))
  }
}