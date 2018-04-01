package org.sample.slowlog

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import org.joda.time.DateTimeZone
import org.sample.slowlog.JsonSupport._
import org.scalatest.{Matchers, WordSpecLike}

class SlowLogCruncherSpec extends WordSpecLike with Matchers {

  implicit val system = ActorSystem()

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  "SlowLogCruncher" should {
    "get time range for given format" in {
      val slowLog = """[2017-11-23 00:00:00,392][WARN ][index.search.slowlog.query] [machine1-data01] [log-2017-10-25][0] took[17.5ms], took_millis[17], types[logs], stats[], search_type[QUERY_THEN_FETCH], total_shards[2400], source[{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"range":{"timestamp":{"gte":"2017-11-22T08:00:00.006Z","lt":"2017-11-23T08:00:00.006Z"}}}]}}}}}], extra_source[],"""

      val x = Source.single(slowLog).via(SlowLogCruncher.transform(DateTimeZone.UTC.getID))
        .runWith(TestSink.probe[Map[String, Any]])
        .request(1)
        .expectNext()
      x.toSortedMap shouldBe Map("aggregation_levels" -> 0, "index" -> "log-2017-10-25", "latency" -> 17, "node" -> "machine1-data01", "query" -> """{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"range":{"timestamp":{"gte":"2017-11-22T08:00:00.006Z","lt":"2017-11-23T08:00:00.006Z"}}}]}}}}}""", "search_type" -> "QUERY_THEN_FETCH", "shard_id" -> "0", "signature" -> -948470321, "size" -> 165, "timerange" -> 86399, "timestamp" -> "2017-11-23T00:00:00.392Z", "total_shards" -> "2400").toSortedMap
    }
  }
  "getAggregationLevels" should {
    "result in 0 when there is no `aggs` keyword" in {
      SlowLogCruncher.getAggregationLevels(Map("a" -> "b")) shouldBe 0
    }

    "get number of levels of a nested aggregation" in {
      val json = """{"query":{"match":{"name":"ledtv"}},"aggs":{"resellers":{"nested":{"path":"resellers"},"aggs":{"min_price":{"min":{"field":"resellers.price"}}}}}}"""
      SlowLogCruncher.getAggregationLevels(Map().fromJSON(json)) shouldBe 2
    }

    "get maximum of number of levels of a nested aggregation when there are multiple sibling aggregation" in {
      val json = """{"aggs":{"stock":{"terms":{"field":"stock"},"aggs":{"customer":{"terms":{"field":"cust_id"},"aggs":{"device":{"terms":{"field":"devi_id"}}}}}},"date":{"terms":{"field":"evt_date"},"aggs":{"customer":{"terms":{"field":"cust_id"},"aggs":{"device":{"terms":{"field":"devi_id"},"aggs":{"type":{"terms":{"field":"type"},"aggs":{"action":{"terms":{"field":"action"},"aggs":{"hr":{"terms":{"field":"hr"}}}}}}}}}}}}}}"""
      SlowLogCruncher.getAggregationLevels(Map().fromJSON(json)) shouldBe 6
    }
  }
}
