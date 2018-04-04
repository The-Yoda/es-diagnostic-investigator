package org.yoda

import org.joda.time.DateTimeZone

import scala.util.Try

object ArgParser {
  def parse(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("-----------------Elasticsearch diagnostic investigator-----------------")

      opt[String]('u', "url").action((url, c) =>
        c.copy(esBaseUrl = url)).text("ES base url. Sample: http://localhost:9200")

      opt[String]('t', "timeZone").action((tz, c) =>
        c.copy(timeZone = tz))
        .validate { tz =>
          if (Try(DateTimeZone.forID(tz).getID).getOrElse("").nonEmpty) success
          else failure(s"Time zone id $tz not recognised. Refer http://joda-time.sourceforge.net/timezones.html.")
        }
        .text("Timezone the logs are in. Refer http://joda-time.sourceforge.net/timezones.html for getting timezone")

      opt[String]('l', "slowLogLocation").required().action((loc, c) =>
        c.copy(slowLogLocation = loc)).text("slowLogLocation. It should be an absolute path.")
    }

    parser.parse(args, Config()) match {
      case Some(config) => config
      case None         => throw new IllegalArgumentException("Invalid arguments!")
    }
  }
}
