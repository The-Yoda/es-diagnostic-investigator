package org.yoda.es

import java.io.File
import java.nio.file.Files
import java.util.{Arrays, Collection}

import org.apache.commons.io.FileUtils
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.InternalSettingsPreparer.{prepareEnvironment => env}
import org.elasticsearch.node.Node
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.transport.Netty4Plugin
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.NonFatal

class MyNode(settings: Settings, plugins: Collection[Class[_ <: Plugin]]) extends Node(env(settings, null), plugins) {}

object ElasticsearchClient {
  private val logger = LoggerFactory.getLogger(getClass)

  private val dataDir = mutable.LinkedHashMap.empty[Int, String]
  private val nodeInfo = mutable.LinkedHashMap.empty[Int, MyNode]

  private val DataDirBasePath = "elasticsearch_data_"

  def startEmbeddedElasticSearch(port: Int): Unit = {
    if (nodeInfo.get(port).nonEmpty && !nodeInfo(port).isClosed) {
      logger.info(s"Embedded ES already running with port $port. Not creating one!")
      return
    }

    val dir: File = Files.createTempDirectory(DataDirBasePath + port).toFile

    logger.info(s"Created elasticsearch data directory in ${dir.getAbsolutePath}")

    dataDir.put(port, dir.getAbsolutePath)

    logger.info(s"Starting embedded elasticsearch with port $port")

    val settings = Settings.builder.put("transport.type", "netty4")
      .put("http.type", "netty4")
      .put("http.enabled", "true")
      .put("path.data", dataDir.toString)
      .put("path.home", "elasticsearch-data")
      .put("http.port", port)
      .put("transport.tcp.port", port + 1)
      .build

    val node = new MyNode(settings, Arrays.asList(classOf[Netty4Plugin]))
    node.start()
    nodeInfo.put(port, node)
  }

  def stopEmbeddedElasticSearch(port: Int): Unit = {
    nodeInfo.get(port).foreach(_.close())
    nodeInfo.remove(port)

    try {
      FileUtils.forceDelete(new File(dataDir(port)))
      dataDir.remove(port)
    } catch {
      case NonFatal(e) => logger.error("dataDir cleanup failed", e)
    }
  }
}
