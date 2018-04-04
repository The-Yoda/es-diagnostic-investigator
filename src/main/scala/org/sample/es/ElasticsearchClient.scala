package org.sample.es

import java.util.{Arrays, Collection}

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.InternalSettingsPreparer.{prepareEnvironment => env}
import org.elasticsearch.node.Node
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.transport.Netty4Plugin
import org.slf4j.LoggerFactory

class MyNode(settings: Settings, plugins: Collection[Class[_ <: Plugin]]) extends Node(env(settings, null), plugins) {}

object ElasticsearchClient {
  val logger = LoggerFactory.getLogger(getClass)

  private var node: Option[MyNode] = None

  def startEmbeddedElasticSearch(port: Int) = {
    logger.info(s"Starting embedded elasticsearch with port $port")
    
    val settings = Settings.builder.put("transport.type", "netty4")
      .put("http.type", "netty4")
      .put("http.enabled", "true")
      .put("path.home", "elasticsearch-data")
      .put("http.port", port)
      .put("transport.tcp.port", port + 1)
      .build
    node = Some(new MyNode(settings, Arrays.asList(classOf[Netty4Plugin])))
    node.get.start()
  }

  def stopEmbeddedElasticSearch(): Unit = {
    node.foreach(_.close())
    node = None
  }
}
