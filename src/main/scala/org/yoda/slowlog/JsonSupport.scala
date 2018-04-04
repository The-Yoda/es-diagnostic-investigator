package org.yoda.slowlog

import com.fasterxml.jackson.databind.DeserializationFeature
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{DefaultFormats, JValue}

import scala.collection.SortedMap
import scala.util.control.NonFatal

object JsonSupport {

  JsonMethods.mapper.configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, false)

  implicit val formats = DefaultFormats

  implicit class EnrichedMap[A, B](map: scala.collection.Map[A, B]) {

    def fromJSON(json: String): Map[String, Any] = Option(json).filter(_.nonEmpty).map(json =>
      try getMap(JsonMethods.parse(json, useBigDecimalForDouble = false, useBigIntForLong = false).values)
      catch {
        case NonFatal(e) => throw new Exception(e.getMessage)
      }
    ).getOrElse(Map.empty[String, Any])

    private def getMap(parsed: JValue#Values): Map[String, Any] = parsed match {
      case _: Map[_, _] => parsed.asInstanceOf[Map[String, Any]]
      case _            => Map("collection" -> parsed)
    }

    def toJSON: String = Serialization.write(map)

    def toSortedMap(implicit ordering: Ordering[A]): SortedMap[A, _] = SortedMap(map.mapValues(toSorted).toSeq: _*)

    private def toSorted(implicit ordering: Ordering[A]): PartialFunction[Any, Any] = {
      case mp: Map[A, _] => SortedMap(mp.mapValues(toSorted).toSeq: _*)
      case lst: List[_]  => lst.map(toSorted)
      case v             => v
    }
  }
}
