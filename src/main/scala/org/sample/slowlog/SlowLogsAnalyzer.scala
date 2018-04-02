package org.sample.slowlog

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling._
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.Supervision.Resume
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.sample.slowlog.GenericTypes.MAP
import org.sample.slowlog.JsonSupport.EnrichedMap
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

object SlowLogsAnalyzer {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem()

  import system.dispatcher

  val http = Http()

  private val settings = ActorMaterializerSettings(system).withSupervisionStrategy({ t =>
    logger.error("Exception from stream", t)
    Resume
  })

  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  private val contentType: ContentType = ContentType.parse("application/x-ndjson") match {
    case Left(_)               => ContentTypes.NoContentType
    case Right(c: ContentType) => c
  }

  private val sink = Sink.fold[(Long, Long), List[Long]]((0l, 0l)) {
    (res, lst) => (res._1 + lst.size, res._2 + lst.count(_ / 100 != 2l))
  }

  def analyze(tz: String, slowLogFile: String, esEndpoint: String = "http://localhost:6393"): Future[(Long, Long)] = {
    println(getBulkEndpoint(esEndpoint), tz, slowLogFile)
    ingestCrunchedSlowLog(tz, slowLogFile, getBulkEndpoint(esEndpoint))
  }

  def ingestCrunchedSlowLog(tz: String, slowLogFile: String, esEndpoint: String) =
    Source
      .fromIterator(() => scala.io.Source.fromFile(new File(slowLogFile)).getLines())
      .via(SlowLogCruncher.crunchData(tz))
      .groupedWithin(1000, 1 second)
      .mapAsyncUnordered(10) {
        seq =>
          val entity = HttpEntity(contentType, seq.map(createRequest).foldLeft(Array.emptyByteArray)(_ ++ _))
          http.singleRequest(HttpRequest(HttpMethods.POST, esEndpoint, entity = entity)).flatMap(parseJSON)
      }.map(parseESResponse)
      .runWith(sink)

  private def getBulkEndpoint(url: String): String = url.stripSuffix("/") + "/_bulk"

  private def parseJSON(r: HttpResponse): Future[MAP] = Unmarshal(r.entity).to[String].fast map Map().fromJSON

  private def parseESResponse(resp: MAP): List[Long] =
    resp("items").asInstanceOf[List[MAP]].map(_ ("index").asInstanceOf[MAP]("status").asInstanceOf[Long])

  private def createRequest(data: MAP): Array[Byte] =
    (Map("index" -> Map("_index" -> "slowlog", "_type" -> "log")).toJSON + "\n" + data.toJSON + "\n").getBytes
}
