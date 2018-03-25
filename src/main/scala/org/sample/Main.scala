package org.sample

import java.io.File
import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.stream.Supervision._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable

object Main extends App {
  SlowLogsAnalyzer.analyze()
}
