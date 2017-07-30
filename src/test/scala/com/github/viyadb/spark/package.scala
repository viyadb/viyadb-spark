package com.github.viyadb

import org.scalatest._

package object spark {

  abstract class UnitSpec extends FlatSpec with Matchers with
    OptionValues with Inside with Inspectors
}
