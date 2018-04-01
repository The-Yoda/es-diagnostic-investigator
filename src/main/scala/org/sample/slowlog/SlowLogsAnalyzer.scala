package org.sample.slowlog

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Supervision.Resume
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.sample.ElasticsearchClient
import org.sample.slowlog.JsonSupport.EnrichedMap
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

object SlowLogsAnalyzer {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem()

  val http = Http()

  private val settings = ActorMaterializerSettings(system).withSupervisionStrategy({ t =>
    logger.error("Exception from stream", t)
    Resume
  })

  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  private val url = "http://localhost:9200/_bulk"

  private val contentType: ContentType = ContentType.parse("application/x-ndjson") match {
    case Left(_)               => ContentTypes.NoContentType
    case Right(c: ContentType) => c
  }

  def analyze(tz: String, slowLogFile: String): Unit = {
    ElasticsearchClient.startEmbeddedElasticSearch()

    Source.fromIterator(() => scala.io.Source.fromFile(new File(slowLogFile)).getLines())
      .via(SlowLogCruncher.transform(tz))
      .groupedWithin(1000, 1 second)
      .mapAsyncUnordered(10) {
        seq =>
          val bin = seq.map(createRequest).foldLeft(Array.emptyByteArray)(_ ++ _)
          http.singleRequest(HttpRequest(HttpMethods.POST, url, entity = HttpEntity(contentType, bin)))
      }
      .to(Sink.foreach(e => println(e.entity.toString))).run()
  }

  def createRequest(data: Map[String, Any]): Array[Byte] =
    (Map("index" -> Map("_index" -> "slowlog", "_type" -> "log")).toJSON + "\n" + data.toJSON + "\n").getBytes
}
