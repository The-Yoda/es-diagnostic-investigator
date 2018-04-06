package org.yoda

import java.io.File

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ArgsParserSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  lazy val currentLocation = getClass.getResource(".").getPath
  lazy val slowLogLocation = getClass.getClassLoader.getResource("slowlog.log").getPath

  val default = Config(slowLog = new File(slowLogLocation))
  val requiredArgs = Array("-l", slowLogLocation)

  "ArgsParser" should {
    "parse slowlog file with options `-l` or `--slowlog`" in {
      ArgsParser.parse(requiredArgs) shouldBe default
      ArgsParser.parse(requiredArgs) shouldBe default
    }

    "throw exception when slowlog file is not a file" in {
      intercept[IllegalArgumentException] {
        ArgsParser.parse(Array("-l", "."))
      }
    }

    "parse and give url with options `-u` or `--url`" in {
      ArgsParser.parse(getArgs("-u", "http://localhost:9600")) shouldBe default.copy(esBaseUrl = "http://localhost:9600")
      ArgsParser.parse(getArgs("--url", "http://localhost:9600")) shouldBe default.copy(esBaseUrl = "http://localhost:9600")
    }

    "parse output mode with option `--output-mode`" in {
      ArgsParser.parse(getArgs("--output-mode", "file")) shouldBe default.copy(outPutMode = "file")
      ArgsParser.parse(getArgs("--output-mode", "stdin")) shouldBe default.copy(outPutMode = "stdin")
    }

    "throw exception when output mode is not file or stdin" in {
      intercept[IllegalArgumentException] {
        ArgsParser.parse(getArgs("--output-mode", "something"))
      }
    }

    "parse output directory with options `--out` or `-o`" in {
      ArgsParser.parse(getArgs("--out", currentLocation)) shouldBe default.copy(out = Some(new File(currentLocation)))
      ArgsParser.parse(getArgs("-o", currentLocation)) shouldBe default.copy(out = Some(new File(currentLocation)))
    }

    "throw exception if directory doesn't exist" in {
      intercept[IllegalArgumentException] {
        ArgsParser.parse(getArgs("--out", ""))
      }
    }

    "throw exception if given location is not a directory" in {
      intercept[IllegalArgumentException] {
        ArgsParser.parse(getArgs("--out", getClass.getClassLoader.getResource("slowlog.log").getPath))
      }
    }

    "parse timezone id with options `-t` or `--timezone`" in {
      println("DEFAULT:  " + default.copy(timeZone = "CET"))
      ArgsParser.parse(getArgs("-t", "CET")) shouldBe default.copy(timeZone = "CET")
      ArgsParser.parse(getArgs("--timezone", "CET")) shouldBe default.copy(timeZone = "CET")
    }

    "throw exception when given invalid timezone as input" in {
      intercept[IllegalArgumentException] {
        ArgsParser.parse(getArgs("--timezone", "ASD"))
      }
    }
  }

  private def getArgs(str: String*): Array[String] = requiredArgs ++ str
}
