package org.sample

import java.io.File
import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.stream.Supervision.Resume
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.joda.time.{DateTime, DateTimeZone}
import org.sample.JsonSupport.EnrichedMap

import scala.collection.{SortedMap, mutable}
import scala.util.hashing.MurmurHash3.stringHash

object SlowLogsAnalyzer {
  implicit val system = ActorSystem()

  private val settings = ActorMaterializerSettings(system).withSupervisionStrategy({ t =>
    println("Exception from stream", t)
    Resume
  })

  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  val slowLogPattern = Pattern.compile("""\[([0-9- :,]*)\]\[.+?\]\[.+?\] \[(.+?)\] \[(.+?)\]\[(.+?)\] took\[.+?\], took_millis\[(.+?)\],.*search_type\[(.+?)\], total_shards\[(.+?)\], source\[(.*)\], extra_source\[(.*)\],\s*""")
  val timeZone = DateTimeZone.getDefault.getID

  def analyze(): Unit = {
    Source.fromIterator(() => scala.io.Source.fromFile(new File("/Users/sujeeva/Desktop/slowlog.log")).getLines())
      .filter(_.contains("index.search.slowlog.query"))
      .map(slowLogPattern.matcher)
      .filter(_.matches())
      .map {
        p =>
          val query = p.group(8)

          mutable.SortedMap("timestamp" -> formatTimestamp(p.group(1)), "node" -> p.group(2), "index" -> p.group(3),
            "shard_id" -> p.group(4), "latency" -> p.group(5), "search_type" -> p.group(6), "total_shards" -> p.group(7),
            "query" -> query, "signature" -> getQuerySignature(query), "size" -> query.size)
      }
      .to(Sink.foreach(println)).run()
  }

  private def formatTimestamp(timeStamp: String): String = {
    val year = timeStamp.take(4).toInt
    val month = timeStamp.slice(5, 7).toInt
    val day = timeStamp.slice(8, 10).toInt
    val hour = timeStamp.slice(11, 13).toInt
    val min = timeStamp.slice(14, 16).toInt
    val sec = timeStamp.slice(17, 19).toInt
    val milli = timeStamp.slice(20, 23).toInt

    val dt = new DateTime(year, month, day, hour, min, sec, milli, DateTimeZone.forID(timeZone))
    dt.toDateTime(DateTimeZone.UTC).toString
  }

  type MAP = SortedMap[String, Any]

  private def getQuerySignature(query: String): String = stringHash(stripValues(query)).toString

  def stripValues(json: String): String = Map.empty[String, Any].fromJSON(json).toSortedMap.mapValues(strip).toJSON

  private val strip: PartialFunction[Any, Any] = {
    case m: MAP     => m.mapValues(strip)
    case l: List[_] => l.map(strip)
    case v@_        => v.getClass.getSimpleName
  }
}
