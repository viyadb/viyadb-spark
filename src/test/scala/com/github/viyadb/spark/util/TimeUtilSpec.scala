package com.github.viyadb.spark.util

import com.github.viyadb.spark.UnitSpec

class TimeUtilSpec extends UnitSpec {

  "TimeUtil" should "convert posix format to Java" in {
    assert(TimeUtil.convertStrptimeFormat("%Y-%m-%dT%H:%M:%S%z") == "yyyy-MM-dd'T'HH:mm:ssZ")
    assert(TimeUtil.convertStrptimeFormat("%Y-%m-%dT%H:%M:%S.%f%z") == "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  }

  "TimeUtil" should "throw exception on unsupported modifier" in {
    assertThrows[IllegalArgumentException](TimeUtil.convertStrptimeFormat("%Y %m %d %w"))
  }

  "TimeUtil" should "support embedded time formats" in {
    assert(TimeUtil.strptime2JavaFormat("posix").parse("1564167545").getTime == 1564167545000L)
    assert(TimeUtil.strptime2JavaFormat("millis").parse("1564167545123").getTime == 1564167545123L)

    val microTime = TimeUtil.strptime2JavaFormat("micros").parse("1564167545123456")
    assert(microTime.getTime == 1564167545123L)
    assert(microTime.getNanos == 123456000L)
  }
}