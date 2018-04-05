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
import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3.stringHash

object ESClient {

  import HttpMethods.POST
  import system.dispatcher

  private val http = Http()

  def insertData(esEndpoint: String): Flow[MAP, List[Long], NotUsed] = {
    val url = getBulkEndpoint(esEndpoint)
    val contentType: ContentType = ContentType.parse("application/x-ndjson").toOption.get

    Flow[MAP].groupedWithin(1000, 1 second)
      .mapAsyncUnordered(10) {
        seq => request(url, seq.map(createBulkRequest).foldLeft(Array.emptyByteArray)(_ ++ _), contentType, POST)
      }.map(parseBulkResponse)
  }

  def queryData(esEndpoint: String, indexName: String, query: String): Future[List[MAP]] = {
    val url = esEndpoint.stripSuffix("/") + "/" + indexName + "/_search"
    request(url, query.getBytes, ContentTypes.`application/json`, POST)
      .map(_("hits").asInstanceOf[MAP]("hits").asInstanceOf[List[MAP]])
  }

  private def getBulkEndpoint(url: String): String = url.stripSuffix("/") + "/_bulk"

  val NL = "\n"

  private def createBulkRequest(data: MAP): Array[Byte] = {
    val jData = data.toJSON
    val metadata = Map("index" -> Map("_index" -> "slowlog", "_type" -> "log", "_id" -> stringHash(jData))).toJSON
    s"""$metadata$NL$jData$NL""".getBytes
  }

  private def parseBulkResponse(resp: MAP): List[Long] =
    resp("items").asInstanceOf[List[MAP]].map(_ ("index").asInstanceOf[MAP]("status").asInstanceOf[Long])

  private def request(url: String, data: Array[Byte], contentType: ContentType, method: HttpMethod): Future[MAP] =
    http.singleRequest(HttpRequest(method, url, entity = HttpEntity(contentType, data))).flatMap(parseJSON)

  private def parseJSON(r: HttpResponse): Future[MAP] = Unmarshal(r.entity).to[String].fast map Map().fromJSON
}
