package org.sample.slowlog

import java.util.Locale
import java.util.function.LongSupplier

import org.elasticsearch.common.joda.{DateMathParser, Joda}
import org.joda.time.DateTimeZone
import org.sample.slowlog.JsonSupport.EnrichedMap
import org.slf4j.LoggerFactory

import scala.collection.SortedMap
import scala.util.control.NonFatal

/**
  * This analyzer takes best effort detection of time range(in seconds) based on gt[e], lt[e] in queries
  * It takes into consideration of `format`, `time_zone` inside range query block
  */
object TimeRangeAnalyzer {

  private val logger = LoggerFactory.getLogger(getClass)
  type MAP = SortedMap[String, Any]
  type MAPUNTYPED = SortedMap[_, _]

  private val gts = List("gt", "gte")
  private val lts = List("lt", "lte")
  private val limitKeys = gts ++ lts
  private val LowerLimit = 946684800l //Jan 1st 2000 In seconds
  private val LowerLimitMillis = 946684800000l //Jan 1st 2000 In milliseconds

  private val DefaultParser = new DateMathParser(Joda.forPattern("strict_date_optional_time||epoch_millis", Locale.ROOT))

  /**
    * Get max time range in seconds
    * @param query Actual query
    * @param queriedTime Time the query was executed. It is now taken from slow log
    * @return
    */
  def getMaxTimeRange(query: MAP, queriedTime: Long): Long = try {
    val timeRanges = getRanges(query).filter(isDateFormat).map(getTimeRange(_, queriedTime))

    if (timeRanges.nonEmpty) timeRanges.max else -1
  } catch {
    case NonFatal(e) =>
      logger.error(s"Error parsing time range for query ${query.toJSON}", e)
      throw e
  }

  /**
    * Get all range query blocks in a elastic query
    * Input:
    * {{{ {"range": { "timestamp": { "gte": "lowerDateValue", "lt": "upperDateValue"}} } }}}
    *
    * Output:
    * {{{ [{ "gte": "lowerDateValue", "lt": "upperDateValue"}] }}}
    */
  private def getRanges(map: MAP): List[MAP] = map.flatMap {
    case (k, v: MAPUNTYPED) if isRange(k, v) => List(v.head._2.asInstanceOf[MAP])
    case (_, v: MAPUNTYPED)                  => getRanges(v.asInstanceOf[MAP])
    case (_, v: List[_])                     => v.filter(_.isInstanceOf[MAPUNTYPED]).map(_.asInstanceOf[MAP]).flatMap(getRanges)
    case _                                   => List.empty[MAP]
  }.toList

  private def isRange(k: String, v: MAPUNTYPED) = k.equals("range") && v.size == 1 && v.head._2.isInstanceOf[MAPUNTYPED]

  private def isDateFormat(range: MAP): Boolean =
    range.contains("format") ||
      range.contains("time_zone") ||
      range.forall(_._2.isInstanceOf[String]) ||
      range.forall {
        case (_, v) if v.isInstanceOf[Long] => v.asInstanceOf[Long] > LowerLimit
        case _                              => true
      }

  def getTimeRange(range: MAP, queriedTime: Long): Long = {

    val parser = getDateParser(range)
    val timeZone = DateTimeZone.forID(range.getOrElse("time_zone", "+00:00").asInstanceOf[String])
    val (lower, includeLower, lowerTZ) = getLowerValue(range, timeZone)
    val (upper, includeUpper, upperTZ) = getUpperValue(range, timeZone, queriedTime)

    val currentTime = new LongSupplier {
      override def getAsLong: Long = queriedTime
    }

    val l = parser.parse(lower, currentTime, !includeLower, lowerTZ) + (if (!includeLower) 1 else 0)
    val u = parser.parse(upper, currentTime, includeUpper, upperTZ) - (if (!includeUpper) 1 else 0)

    (u - l) / 1000l
  }

  private def getDateParser(range: MAP): DateMathParser =
    if (range.contains("format")) new DateMathParser(Joda.forPattern(range("format").asInstanceOf[String], Locale.ROOT))
    else DefaultParser

  private def getLowerValue(range: MAP, timeZone: DateTimeZone): (String, Boolean, DateTimeZone) = {
    val lowerKey = gts.filter(range.contains)

    //Giving the lower limit as year 2000. TODO: Get retention as input and use that as lower limit
    val lowerValue = if (lowerKey.isEmpty) LowerLimitMillis else range(lowerKey.head)

    (lowerValue.toString, range.contains("gte"), if (lowerKey.isEmpty) DateTimeZone.UTC else timeZone)
  }

  private def getUpperValue(range: MAP, timeZone: DateTimeZone, queriedTime: Long): (String, Boolean, DateTimeZone) = {
    val upperKey = lts.filter(range.contains)
    val upperValue = if (upperKey.isEmpty) queriedTime else range(upperKey.head)

    (upperValue.toString, range.contains("lte"), if (upperKey.isEmpty) DateTimeZone.UTC else timeZone)
  }
}
