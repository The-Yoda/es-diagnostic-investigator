package org.yoda.slowlog

import org.yoda.slowlog.JsonSupport.EnrichedMap
import org.scalatest.{Matchers, WordSpecLike}

class JsonSupportSpec extends WordSpecLike with Matchers {

  "JsonSupport" should {
    "support json parsing in Map" in {
      Map().fromJSON("""{"a":"b"}""") shouldBe Map("a" -> "b")
    }

    "support `Map` to json conversion" in {
      Map("a" -> "b").toJSON shouldBe """{"a":"b"}"""
    }

    "support `Map` to `SortedMap`" in {
      Map().fromJSON("""{"b":"c", "a":"b"}""").keys.toList shouldBe List("b", "a")
      Map().fromJSON("""{"b":"c", "a":"b"}""").toSortedMap.keys.toList shouldBe List("a", "b")
    }

    "support `SortedMap` to sorted json" in {
      Map().fromJSON("""{"b":"c","a":"b"}""").toJSON shouldBe """{"b":"c","a":"b"}"""
      Map().fromJSON("""{"b":"c", "a":"b"}""").toSortedMap.toJSON shouldBe """{"a":"b","b":"c"}"""
    }
  }
}