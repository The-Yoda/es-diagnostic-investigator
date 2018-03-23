package org.sample

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.joda.time.{DateTime, DateTimeZone}

object Main extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  import java.util.regex.Pattern

  println("Running main!")

  val pattern = Pattern.compile("""\[([0-9- :,]*)\]\[.+?\]\[.+?\] \[(.+?)\] \[(.+?)\]\[(.+?)\] took\[.+?\], took_millis\[(.+?)\],.*search_type\[(.+?)\], total_shards\[(.+?)\], source\[(.*)\], extra_source\[(.*)\],\s*""")

  val text = """[2017-11-23 00:00:00,392][WARN ][index.search.slowlog.query] [slccalmelastic0006_cal-root-insights-data01] [cal-root-2017-10-25][0] took[17.5ms], took_millis[17], types[logs], stats[], search_type[QUERY_THEN_FETCH], total_shards[2400], source[{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"range":{"timestamp":{"gte":"2017-11-22T08:00:00.006Z","lt":"2017-11-23T08:00:00.006Z"}}},{"terms":{"pool":["switchapiserv"]}}],"should":[{"terms":{"name":["ppaas_1_2.v1.payment-networks.card-debits.POST","ppaas_1_2.v1.payment-networks.card-debits.id.reverse.POST"]}}]}}}},"aggs":{"payload.m_txn_status":{"terms":{"order":{"_count":"desc"},"size":100,"field":"payload.m_txn_status"}},"payload.m_curr":{"terms":{"order":{"_count":"desc"},"size":100,"field":"payload.m_curr"}},"payload.m_instruction_type":{"terms":{"order":{"_count":"desc"},"size":100,"field":"payload.m_instruction_type"}},"payload.m_txn_type":{"terms":{"order":{"_count":"desc"},"size":100,"field":"payload.m_txn_type"}}}}], extra_source[],"""
  println(getCurrentTime("2017-11-23 00:00:00,392"))

  Source.fromIterator(() => scala.io.Source.fromFile(new File("/Users/sujeeva/Desktop/slowlog.log")).getLines())
    .filter(_.contains("index.search.slowlog.query"))
    .map(pattern.matcher)
    .filter(_.matches())
    .map {
      p =>
        Map("timestamp" -> getCurrentTime(p.group(1)), "node" -> p.group(2), "index" -> p.group(3), "shard_id" -> p.group(4),
          "latency" -> p.group(5), "search_type" -> p.group(6), "total_shards" -> p.group(7), "query" -> p.group(8))
    }
    .to(Sink.foreach(println)).run()

  val timeZone = DateTimeZone.getDefault.getID

  private def getCurrentTime(timeStamp: String): String = {
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
}