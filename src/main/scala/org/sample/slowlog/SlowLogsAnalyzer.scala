package org.sample.slowlog

import java.io.File

import akka.actor.ActorSystem
import akka.stream.Supervision.Resume
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.slf4j.LoggerFactory

object SlowLogsAnalyzer {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem()

  private val settings = ActorMaterializerSettings(system).withSupervisionStrategy({ t =>
    logger.error("Exception from stream", t)
    Resume
  })

  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  def analyze(tz: String): Unit = {
    Source.fromIterator(() => scala.io.Source.fromFile(new File("/Users/sujeeva/Desktop/slowlog.log")).getLines())
      .via(SlowLogCruncher.transform(tz))
      .to(Sink.foreach(println)).run()
  }

}
