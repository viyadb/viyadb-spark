package com.github.viyadb.spark.util

import java.util.TimeZone

import com.github.viyadb.spark.UnitSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.testcontainers.containers.GenericContainer

class ConsulClientSpec extends UnitSpec with BeforeAndAfterAll with BeforeAndAfter {

  private var consul: GenericContainer[_] = _

  override def beforeAll() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    consul = new GenericContainer("consul:latest")
      .withExposedPorts(8500)
    consul.start()
  }

  override def afterAll(): Unit = {
    if (consul != null) {
      consul.stop()
    }
  }

  "ConsulClient" should "read written values" in {
    val consulClient = new ConsulClient(consul.getContainerIpAddress, consul.getFirstMappedPort)

    consulClient.kvPut("/first", "foo")
    assert(consulClient.kvGet("/first") == "foo")
    assert(consulClient.kvGet("first") == "foo")

    consulClient.kvPut("second", "bar")
    assert(consulClient.kvList("/") == Seq("first", "second"))
  }
}