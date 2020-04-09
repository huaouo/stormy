package com.huaouo.stormy.workerprocess;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.GuiceModule;
import com.huaouo.stormy.workerprocess.controller.TransmitTupleController;
import com.huaouo.stormy.wrapper.ZooKeeperConnection;
import io.grpc.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WorkerProcessServer {

    private Server server;
    private int port;

    public WorkerProcessServer(int port) {
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
                .addService(injector.getInstance(TransmitTupleController.class))
                .build()
                .start();

        log.info("Server started, listening on tcp port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                WorkerProcessServer.this.stop();
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

        injector.getInstance(ZooKeeperConnection.class).close();
    }
}
