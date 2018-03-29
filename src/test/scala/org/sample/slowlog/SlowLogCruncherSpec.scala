package org.sample.slowlog

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import org.joda.time.DateTimeZone
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.SortedMap
import org.sample.slowlog.JsonSupport._

class SlowLogCruncherSpec extends WordSpecLike with Matchers {

  implicit val system = ActorSystem()

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  "SlowLogCruncher" should {
    "get time range for given format" in {
      val slowLog = """[2017-11-23 00:00:00,392][WARN ][index.search.slowlog.query] [machine1-data01] [log-2017-10-25][0] took[17.5ms], took_millis[17], types[logs], stats[], search_type[QUERY_THEN_FETCH], total_shards[2400], source[{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"range":{"timestamp":{"gte":"2017-11-22T08:00:00.006Z","lt":"2017-11-23T08:00:00.006Z"}}}]}}}}}], extra_source[],"""

      val x = Source.single(slowLog).via(SlowLogCruncher.transform(DateTimeZone.UTC.getID))
        .runWith(TestSink.probe[SortedMap[String, Any]])
        .request(1)
        .expectNext()
      x shouldBe Map("index" -> "log-2017-10-25", "latency" -> 17, "node" -> "machine1-data01", "query" -> """{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"range":{"timestamp":{"gte":"2017-11-22T08:00:00.006Z","lt":"2017-11-23T08:00:00.006Z"}}}]}}}}}""", "search_type" -> "QUERY_THEN_FETCH", "shard_id" -> "0", "signature" -> -948470321, "size" -> 165, "timerange" -> 86399, "timestamp" -> "2017-11-23T00:00:00.392Z", "total_shards" -> "2400").toSortedMap
    }
  }
}
