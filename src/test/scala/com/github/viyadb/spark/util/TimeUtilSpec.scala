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
}