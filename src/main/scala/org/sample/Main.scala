package org.sample

import org.sample.es.ElasticsearchClient._
import org.sample.slowlog.SlowLogsAnalyzer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import scala.util.{Failure, Success}


case class Config(slowLogLocation: String = "", esBaseUrl: String = "", timeZone: String = "PST8PDT")

object Main extends App {

  override def main(args: Array[String]): Unit = {
    try {
      val config = ArgParser.parse(args)
      val useEmbeddedES = config.esBaseUrl.isEmpty

      val esBaseUrl = if (useEmbeddedES) "http://localhost:6393" else config.esBaseUrl

      if (useEmbeddedES) startEmbeddedElasticSearch(6393)

      val analyzed = SlowLogsAnalyzer.analyze(config.timeZone, config.slowLogLocation, esBaseUrl)

      analyzed.onComplete {
        case Success(res) => println(res)
        case Failure(t)   => t.printStackTrace()
      }
      analyzed.onComplete { _ =>
        stopEmbeddedElasticSearch()
        Commons.system.terminate()
      }

    } catch {
      case NonFatal(e) => e.printStackTrace()
    }
  }
}
