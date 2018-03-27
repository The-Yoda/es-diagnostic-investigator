package org.sample.slowlog

import org.sample.slowlog.JsonSupport.EnrichedMap
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.SortedMap

class TimeRangeAnalyzerSpec extends WordSpecLike with Matchers {

  import TimeRangeAnalyzer._

  val queriedTime = 1511456400000l //2017-11-23T17:00:00.000Z

  "getMaxTimeRange" should {
    "get time range for given format" in {
      val json = """{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"range":{"timestamp":{"gte":"2017-11-22T08:00:00.006Z","lt":"2017-11-23T08:00:00.006Z"}}},{"terms":{"xxx":["yyy"]}}],"should":[{"terms":{"name":["xyz","zxy"]}}]}}}}}"""
      getMaxTimeRange(getSortedMap(json), queriedTime) shouldBe 86399
    }
    "get max time range out of different ranges" in {
      val json = """{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"range":{"timestamp":{"gte":"2017-11-22T08:00:00.006Z","lte":"2017-11-23T08:00:01.006Z"}}},{"range":{"timestamp":{"gte":"2017-11-22T08:00:00.006Z","lt":"2017-11-23T08:00:00.006Z"}}},{"terms":{"xxx":["yyy"]}}],"should":[{"terms":{"name":["xyz","zxy"]}}]}}}}}"""
      getMaxTimeRange(getSortedMap(json), queriedTime) shouldBe 86401
    }

    "get -1 when there is no range in the query" in {
      val json = """{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"terms":{"xxx":["yyy"]}}],"should":[{"terms":{"name":["xyz","zxy"]}}]}}}}}"""
      getMaxTimeRange(getSortedMap(json), queriedTime) shouldBe -1
    }

    "get -1 when the range has no date in the query" in {
      val json = """{"from":0,"size":2000,"query":{"filtered":{"filter":{"bool":{"must":[{"range":{"timestamp":{"gte":10,"lt":20}}},{"terms":{"xxx":["yyy"]}}],"should":[{"terms":{"name":["xyz","zxy"]}}]}}}}}"""
      getMaxTimeRange(getSortedMap(json), queriedTime) shouldBe -1
    }
  }

  "getTimeRange" should {
    "get time range for default(UTC) format" in {
      getTimeRange(SortedMap("gte" -> "2017-11-22T08:00:00.006Z", "lt" -> "2017-11-23T08:00:00.006Z"), queriedTime) shouldBe 86399
    }

    "get time range for given format" in {
      getTimeRange(SortedMap("gte" -> "2017-11-22", "lte" -> "2017-11-23", "format" -> "yyyy-MM-dd"), queriedTime) shouldBe 172799
    }

    "get time range for epoch millis" in {
      getTimeRange(SortedMap("lte" -> "1511456399999", "gte" -> "1511454600000"), queriedTime) shouldBe 1799
    }

    "get time range for given time zone" in {
      getTimeRange(SortedMap("gte" -> "2017-11-22T08:00", "lte" -> "2017-11-23T08:00", "time_zone" -> "America/Los_Angeles"), queriedTime) shouldBe 86459
    }

    "get time range for given time zone with no upper bound" in {
      getTimeRange(SortedMap("gte" -> "2017-11-23T08:00:00.006Z", "time_zone" -> "America/Los_Angeles"), queriedTime) shouldBe 32399
    }
    
    "get time range for given time zone with no lower bound" in {
      //should get time difference since year 2000 to upper bound
      getTimeRange(SortedMap("lte" -> "2017-11-23T08:00:00.006Z", "time_zone" -> "America/Los_Angeles"), queriedTime) shouldBe 564739200
    }

  }

  private def getSortedMap(json: String) = Map().fromJSON(json).toSortedMap

}
