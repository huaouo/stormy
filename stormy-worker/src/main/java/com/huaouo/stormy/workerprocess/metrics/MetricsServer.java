// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.metrics;

import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.OutputStream;
import java.net.InetSocketAddress;

@Slf4j
@Singleton
public class MetricsServer {

    @Inject
    PrometheusMeterRegistry prometheusRegistry;

    @Inject
    ZooKeeperConnection zkConn;

    public void asyncStart(String topologyName, String workerProcessId) {
        try {
            prometheusRegistry.config().commonTags("topology", topologyName, "id", workerProcessId);
            new JvmMemoryMetrics().bindTo(prometheusRegistry);
            new JvmGcMetrics().bindTo(prometheusRegistry);
            new ProcessorMetrics().bindTo(prometheusRegistry);
            new JvmThreadMetrics().bindTo(prometheusRegistry);
            HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            server.start();

            int port = server.getAddress().getPort();
            String ip = SharedUtil.getIp();
            zkConn.create("/exporter/" + ip + ":" + port, null, CreateMode.EPHEMERAL);
        } catch (Throwable t) {
            log.error("Failed to start prometheus server: " + t.toString());
            System.exit(-1);
        }
    }
}
