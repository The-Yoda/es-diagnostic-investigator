package org.sample;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class ElasticsearchClient {

    static Node node = null;

    public static Node startEmbeddedElasticSearch() throws NodeValidationException {
        node = new MyNode(
                Settings.builder()
                        .put("transport.type", "netty4")
                        .put("http.type", "netty4")
                        .put("http.enabled", "true")
                        .put("path.home", "elasticsearch-data")
                        .build(), Arrays.asList(Netty4Plugin.class));

        node.start();
        return node;
    }

    static class MyNode extends Node {
        public MyNode(Settings preparedSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), classpathPlugins);
        }
    }

    public static void stopEmbeddedElasticSearch() throws IOException {
        if (node != null) node.close();
    }
}
