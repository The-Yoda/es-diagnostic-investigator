package org.yoda

import org.yoda.es.ElasticsearchClient._
import org.yoda.slowlog.SlowLogsAnalyzer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import scala.util.{Failure, Success}


case class Config(slowLogLocation: String = "", esBaseUrl: String = "", timeZone: String = "PST8PDT")

object Main extends App {

  private val EmbeddedESPort = 6393
  private val EmbeddedESUrl = s"http://localhost:$EmbeddedESPort"

  override def main(args: Array[String]): Unit = {
    try {
      val config = ArgParser.parse(args)

      val useEmbeddedES = config.esBaseUrl.isEmpty
      val esBaseUrl = if (useEmbeddedES) EmbeddedESUrl else config.esBaseUrl

      if (useEmbeddedES) startEmbeddedElasticSearch(EmbeddedESPort)

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
