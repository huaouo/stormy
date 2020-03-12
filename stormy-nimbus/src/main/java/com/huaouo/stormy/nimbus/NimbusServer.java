// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus;

import dagger.grpc.server.NettyServerModule;
import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NimbusServer {

    private Server server;
    private int port;

    public NimbusServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        NimbusComponent component = DaggerNimbusComponent.builder()
                .nettyServerModule(NettyServerModule.bindingToPort(port))
                .build();
        server = component.server();
        server.start();

        log.info("Server started, listening on tcp port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                NimbusServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            log.info("Server shut down");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            log.info("Server shut down");
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
            log.info("Server shut down");
        }
    }
}
