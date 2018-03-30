package org.sample.slowlog

import java.util.regex.Pattern

import akka.stream.scaladsl.Flow
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sample.slowlog.JsonSupport.EnrichedMap

import scala.collection.SortedMap
import scala.util.hashing.MurmurHash3.stringHash

object SlowLogCruncher {

  type MAP = Map[String, Any]
  private val SlowLogPattern = Pattern.compile("""\[([0-9- :,]*)\]\[.+?\]\[.+?\] \[(.+?)\] \[(.+?)\]\[(.+?)\] took\[.+?\], took_millis\[(.+?)\],.*search_type\[(.+?)\], total_shards\[(.+?)\], source\[(.*)\], extra_source\[(.*)\],\s*""")
  private val SlowLogDatePattern = "yyyy-MM-dd HH:mm:ss,SSS"

  def transform(tz: String) = Flow[String]
    .filter(_.contains("index.search.slowlog.query"))
    .map(SlowLogPattern.matcher)
    .filter(_.matches())
    .map {
      p =>
        val rawQuery = p.group(8)
        val query = Map().fromJSON(p.group(8))
        val queriedTime = formatTimestamp(p.group(1), tz)

        SortedMap("timestamp" -> queriedTime.toDateTime(DateTimeZone.UTC).toString, "node" -> p.group(2),
          "index" -> p.group(3), "shard_id" -> p.group(4), "latency" -> p.group(5).toLong, "search_type" -> p.group(6),
          "total_shards" -> p.group(7), "query" -> query.toJSON, "signature" -> getQuerySignature(rawQuery),
          "size" -> rawQuery.size, "timerange" -> TimeRangeAnalyzer.getMaxTimeRange(query, queriedTime.getMillis),
          "aggregation_levels" -> getAggregationLevels(query))
    }

  private def formatTimestamp(timeStamp: String, tz: String): DateTime =
    DateTime.parse(timeStamp, DateTimeFormat.forPattern(SlowLogDatePattern).withZone(DateTimeZone.forID(tz)))

  private def getQuerySignature(query: String): Int = stringHash(replaceValueswithType(query))

  /**
    * Strip all the values and replace with type to identify unique queries
    *
    * @param str json to be stripped
    * @return json with values stripped
    */
  private def replaceValueswithType(str: String): String = Map().fromJSON(str).toSortedMap.mapValues(strip).toJSON

  private val strip: PartialFunction[Any, Any] = {
    case m: SortedMap[String, Any] => m.mapValues(strip)
    case l: List[_]                => l.map(strip)
    case v@_                       => v.getClass.getSimpleName
  }

  def getAggregationLevels(query: MAP): Int = query.map {
    case (k, v) if k.equals("aggs") && v.isInstanceOf[MAP] => 1 + getAggregationLevels(v.asInstanceOf[MAP])
    case (_, v) if v.isInstanceOf[MAP]                     => getAggregationLevels(v.asInstanceOf[MAP])
    case _                                                 => 0
  }.max
}
