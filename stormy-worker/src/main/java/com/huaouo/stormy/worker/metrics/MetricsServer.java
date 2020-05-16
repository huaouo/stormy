// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.OutputStream;
import java.net.InetSocketAddress;

@Slf4j
@Singleton
public class MetricsServer {

    @Inject
    PrometheusMeterRegistry prometheusRegistry;

    public void asyncStart() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(5001), 0);
            server.createContext("/", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            server.start();
        } catch (Throwable t) {
            log.error("Failed to start prometheus server: " + t.toString());
            System.exit(-1);
        }
    }
}
