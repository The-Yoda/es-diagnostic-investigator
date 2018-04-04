package org.yoda.es

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.scaladsl.Flow
import org.yoda.Commons._
import org.yoda.slowlog.GenericTypes.MAP
import org.yoda.slowlog.JsonSupport.EnrichedMap

import scala.concurrent.Future

object ESInsert {

  import system.dispatcher

  private val NDJson = "application/x-ndjson"
  private val contentType: ContentType = ContentType.parse(NDJson).toOption.get
  private val http = Http()

  def insertJSON(esEndpoint: String): Flow[Seq[MAP], List[Long], NotUsed] = {
    val url = getBulkEndpoint(esEndpoint)
    Flow[Seq[MAP]].mapAsyncUnordered(10) {
      seq =>
        val entity = HttpEntity(contentType, seq.map(createRequest).foldLeft(Array.emptyByteArray)(_ ++ _))
        http.singleRequest(HttpRequest(HttpMethods.POST, url, entity = entity)).flatMap(parseJSON)
    }.map(parseESResponse)
  }

  private def getBulkEndpoint(url: String): String = url.stripSuffix("/") + "/_bulk"

  private def parseJSON(r: HttpResponse): Future[MAP] = Unmarshal(r.entity).to[String].fast map Map().fromJSON

  private def parseESResponse(resp: MAP): List[Long] =
    resp("items").asInstanceOf[List[MAP]].map(_ ("index").asInstanceOf[MAP]("status").asInstanceOf[Long])

  private def createRequest(data: MAP): Array[Byte] =
    (Map("index" -> Map("_index" -> "slowlog", "_type" -> "log")).toJSON + "\n" + data.toJSON + "\n").getBytes
}
