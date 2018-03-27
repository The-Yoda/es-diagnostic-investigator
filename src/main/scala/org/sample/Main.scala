package org.sample

import org.joda.time.DateTimeZone
import org.sample.slowlog.SlowLogsAnalyzer

object Main extends App {
  SlowLogsAnalyzer.analyze(DateTimeZone.forID("America/Los_Angeles").getID)
}
