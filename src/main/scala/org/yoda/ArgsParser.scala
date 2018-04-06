package org.yoda

import java.io.File

import org.joda.time.DateTimeZone

import scala.util.Try

object ArgsParser {
  def parse(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("-----------------Elasticsearch diagnostic investigator-----------------")

      opt[String]('u', "url")
        .valueName("<http://{host}:{port}>")
        .action((url, c) =>
          c.copy(esBaseUrl = url)).text("ES base url. Sample: http://localhost:9200")

      opt[String]("output-mode")
        .valueName("<stdin> or <file>")
        .action((mode, c) =>
          c.copy(outPutMode = mode)).text("Output mode to write the diagnostic result to.")
        .validate { mode => if (List("stdin", "file").contains(mode)) success else failure("Invalid output mode") }

      opt[File]('o', "out").valueName("<file>")
        .validate { f =>
          if (f.exists()) {
            if (f.canWrite && f.isDirectory) success
            else failure("Given location is not a directory. Permission denied to write")
          } else failure("Directory does not exist")
        }
        .action((f, c) => c.copy(out = Some(f)))
        .text("Location of diagnostic result file.")

      opt[String]('t', "timezone")
        .action((tz, c) => c.copy(timeZone = tz))
        .validate { tz =>
          if (Try(DateTimeZone.forID(tz).getID).getOrElse("").nonEmpty) success
          else failure(s"Time zone id $tz not recognised. Refer http://joda-time.sourceforge.net/timezones.html.")
        }
        .text("Timezone the logs are in. Refer http://joda-time.sourceforge.net/timezones.html for getting timezone")

      opt[File]('l', "slowlog").required().valueName("<file>")
        .action((sl, c) => c.copy(slowLog = sl)).text("slowLogLocation. It should be an absolute path.")
        .validate { f => if (f.exists() && !f.isDirectory) success else failure("Slowlog file does not exist") }
    }

    parser.parse(args, Config()) match {
      case Some(config) => config
      case None         => throw new IllegalArgumentException("Invalid arguments!")
    }
  }
}
