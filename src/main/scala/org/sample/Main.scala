package org.sample

import org.joda.time.DateTimeZone
import org.sample.slowlog.SlowLogsAnalyzer

object Main extends App {
  val timeZone = DateTimeZone.forID("America/Los_Angeles").getID
  val slowLogFile = "/Users/sujeeva/Desktop/slowlogs/slowlogs.log"
  SlowLogsAnalyzer.analyze(timeZone, slowLogFile)
}
