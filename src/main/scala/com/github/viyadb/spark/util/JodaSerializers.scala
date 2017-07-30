package com.github.viyadb.spark.util

import org.joda.time._
import org.json4s._
import org.json4s.JsonAST.{JNull, JString}

/**
  * JSON serializers for Joda time classes that handles intervals better
  * than the original serializer from json4s-ext.
  */
object JodaSerializers {

  val all = List(DurationSerializer, PeriodSerializer, IntervalSerializer())

  case object DurationSerializer extends CustomSerializer[Duration](_ => ( {
    case JString(s) => Duration.parse(s)
    case JNull => null
  }, {
    case d: Duration => JString(d.toString)
  }
  ))

  case object PeriodSerializer extends CustomSerializer[Period](_ => ( {
    case JString(s) => Period.parse(s)
    case JNull => null
  }, {
    case p: Period => JString(p.toString)
  }
  ))

  object IntervalSerializer {
    def apply() = new ClassSerializer(new ClassType[Interval, String]() {
      def unwrap(i: String)(implicit format: Formats) = Interval.parse(i)
      def wrap(i: Interval)(implicit format: Formats) = i.toString
    })
  }

  private[JodaSerializers] trait ClassType[A, B] {
    def unwrap(b: B)(implicit format: Formats): A
    def wrap(a: A)(implicit format: Formats): B
  }

  case class ClassSerializer[A : Manifest, B : Manifest](t: ClassType[A, B]) extends Serializer[A] {
    private val Class = implicitly[Manifest[A]].runtimeClass

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), A] = {
      case (TypeInfo(Class, _), json) => json match {
        case JNull => null.asInstanceOf[A]
        case xs: JValue if (xs.extractOpt[B].isDefined) => t.unwrap(xs.extract[B])
        case value => throw new MappingException(s"Can't convert $value to $Class")
      }
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case a: A if a.asInstanceOf[AnyRef].getClass == Class => Extraction.decompose(t.wrap(a))
    }
  }
}
