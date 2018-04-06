package org.yoda.slowlog

import java.io.File

import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.LoggerFactory
import org.yoda.Commons._
import org.yoda.es.ESClient
import org.yoda.slowlog.JsonSupport.EnrichedMap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object SlowLogsAnalyzer {
  private val logger = LoggerFactory.getLogger(getClass)

  private val sink = Sink.fold[(Long, Long), List[Long]]((0l, 0l)) {
    (res, lst) => (res._1 + lst.size, res._2 + lst.count(_ / 100 != 2l))
  }

  def analyze(tz: String, slowLog: File, esEndpoint: String) = {
    val f = ingestCrunchedSlowLog(tz, slowLog, esEndpoint)
    f.foreach { _ => querySlowLog(esEndpoint, "slowlog").foreach(_.foreach(println)) }
    f
  }

  def ingestCrunchedSlowLog(tz: String, slowLog: File, esEndpoint: String): Future[(Long, Long)] =
    Source
      .fromIterator(() => scala.io.Source.fromFile(slowLog).getLines())
      .via(SlowLogCruncher.crunchData(tz))
      .via(ESClient.insertData(esEndpoint))
      .runWith(sink)

  def querySlowLog(esEndpoint: String, indexName: String): Future[List[String]] = {
    val query = """{"sort" : [{ "latency":"desc"}, {"aggregation_levels":"desc"}, {"timerange":"desc"}, {"total_shards":"desc"}]}"""
    ESClient.queryData(esEndpoint, indexName, query).map(_.map(_.toJSON))
  }
}
