package org.sample

import java.util.{Arrays, Collection}

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.Node
import org.elasticsearch.node.InternalSettingsPreparer.{prepareEnvironment => env}
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.transport.Netty4Plugin

class MyNode(settings: Settings, plugins: Collection[Class[_ <: Plugin]]) extends Node(env(settings, null), plugins) {}

object ElasticsearchClient {
  private var node: Option[MyNode] = None

  def startEmbeddedElasticSearch() = {
    val settings = Settings.builder.put("transport.type", "netty4")
      .put("http.type", "netty4")
      .put("http.enabled", "true")
      .put("path.home", "elasticsearch-data")
      .build
    node = Some(new MyNode(settings, Arrays.asList(classOf[Netty4Plugin])))
    node.get.start()
  }

  def stopEmbeddedElasticSearch(): Unit = node.foreach(_.close())
}
