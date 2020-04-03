// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.GuiceModule;
import com.huaouo.stormy.master.controller.ManageTopologyController;
import com.huaouo.stormy.master.controller.ProvideJarController;
import com.huaouo.stormy.provider.ZooKeeperConnection;
import io.grpc.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MasterServer {

    private Server server;
    private int port;

    public MasterServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        Injector injector = Guice.createInjector(new GuiceModule());

        server = ServerBuilder.forPort(port)
                .intercept(new ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                            ServerCall<ReqT, RespT> call, Metadata headers,
                            ServerCallHandler<ReqT, RespT> next) {
                        call.setCompression("gzip");
                        return next.startCall(call, headers);
                    }
                })
                .addService(injector.getInstance(ManageTopologyController.class))
                .addService(injector.getInstance(ProvideJarController.class))
                .build()
                .start();

        log.info("Server started, listening on tcp port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                MasterServer.this.stop();
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
            cleanUpSingletonResources();
            log.info("Server shut down");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            cleanUpSingletonResources();
            log.info("Server shut down");
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
            cleanUpSingletonResources();
            log.info("Server shut down");
        }
    }

    private void cleanUpSingletonResources() {
        log.info("Cleaning up global resources");
        Injector injector = Guice.createInjector(new GuiceModule());

        // When Runtime.addShutdownHook() in start() is invoked, the following resources
        // have already constructed properly. Therefore, they're safe to release.
        injector.getInstance(ZooKeeperConnection.class).close();
    }
}
