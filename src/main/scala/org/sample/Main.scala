package org.sample

import org.joda.time.DateTimeZone
import org.sample.es.ElasticsearchClient._
import org.sample.slowlog.SlowLogsAnalyzer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object Main extends App {

  try {
    startEmbeddedElasticSearch(6393)

    val timeZone = DateTimeZone.forID("PST8PDT").getID
    val slowLogFile = "/Users/sujeeva/Desktop/slowlogs/slowlog.log"

    SlowLogsAnalyzer.analyze(timeZone, slowLogFile) onComplete {
      case Success(res) =>
        println(res)
        stopEmbeddedElasticSearch()
      case Failure(t)   =>
        t.printStackTrace()
        stopEmbeddedElasticSearch()
    }

  } catch {
    case NonFatal(e) => e.printStackTrace()
  }
}
