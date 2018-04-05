package org.yoda.slowlog

import java.io.File

import akka.stream.scaladsl.{Sink, Source}
import org.yoda.Commons._
import org.yoda.es.ESInsert
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

object SlowLogsAnalyzer {
  private val logger = LoggerFactory.getLogger(getClass)

  private val sink = Sink.fold[(Long, Long), List[Long]]((0l, 0l)) {
    (res, lst) => (res._1 + lst.size, res._2 + lst.count(_ / 100 != 2l))
  }

  def analyze(tz: String, slowLogFile: String, esEndpoint: String): Future[(Long, Long)] = {
    ingestCrunchedSlowLog(tz, slowLogFile, esEndpoint)
  }

  def ingestCrunchedSlowLog(tz: String, slowLogFile: String, esEndpoint: String) =
    Source
      .fromIterator(() => scala.io.Source.fromFile(new File(slowLogFile)).getLines())
      .via(SlowLogCruncher.crunchData(tz))
      .groupedWithin(1000, 1 second)
      .via(ESInsert.insertJSON(esEndpoint))
      .runWith(sink)

}
