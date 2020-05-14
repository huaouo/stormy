// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.api.IOperator;
import com.huaouo.stormy.api.stream.DynamicSchema;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.shared.GuiceModule;
import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import com.huaouo.stormy.workerprocess.controller.TransmitTupleController;
import com.huaouo.stormy.workerprocess.thread.ComputedOutput;
import com.huaouo.stormy.workerprocess.thread.ComputeThread;
import com.huaouo.stormy.workerprocess.thread.TransmitTupleClientThread;
import com.huaouo.stormy.workerprocess.topology.OperatorLoader;
import io.grpc.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
@Singleton
public class WorkerProcessServer {

    @Inject
    private TransmitTupleController messageReceiver;

    @Inject
    private TransmitTupleClientThread messageSender;

    @Inject
    private ZooKeeperConnection zkConn;

    private ExecutorService threadPool = Executors.newCachedThreadPool(r -> {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setDaemon(true);
        return t;
    });

    private Server server;

    private int threadNum;
    private String topologyName;
    private String taskName;
    private String inbound;

    public void start(String jarPath, String taskFullName) throws Throwable {
        decodeTaskFullName(taskFullName);

        URL jarUrl = Paths.get(jarPath).toUri().toURL();
        Class<? extends IOperator> opClass = new OperatorLoader().load(jarUrl, taskName);
        Map<String, DynamicSchema> outboundSchemaMap = registerOutboundSchemas(opClass);

        BlockingQueue<byte[]> inboundQueue = messageReceiver.getInboundQueue();
        int port = startGrpcServer();
        registerInboundStream(port);

        messageSender.initWithOutbounds(outboundSchemaMap.keySet());
        BlockingQueue<ComputedOutput> outboundQueue = messageSender.getOutboundQueue();

        DynamicSchema inboundSchema = blockUntilInboundSchemaAvailable();
        threadPool.submit(messageSender);
        for (int i = 0; i < threadNum; ++i) {
            threadPool.submit(new ComputeThread(opClass, inboundSchema,
                    outboundSchemaMap, inboundQueue, outboundQueue));
        }
        // TODO: add thread monitor as new thread
    }

    @Data
    private static class BytesWrapper {
        private byte[] bytes;
    }

    private DynamicSchema blockUntilInboundSchemaAvailable() throws Throwable {
        BytesWrapper wrapper = new BytesWrapper();
        CountDownLatch barrier = new CountDownLatch(1);
        String inboundPath = "/stream/" + inbound;
        wrapper.setBytes(zkConn.getBytesAndWatch(inboundPath, e -> {
            if (e.getType() == Watcher.Event.EventType.NodeDataChanged) {
                wrapper.setBytes(zkConn.getBytesAndWatch(inboundPath, null));
                barrier.countDown();
            }
        }));

        if (wrapper.getBytes() == null) {
            barrier.await();
        }
        return DynamicSchema.parseFrom(wrapper.getBytes());
    }

    private Map<String, DynamicSchema> registerOutboundSchemas(Class<? extends IOperator> opClass) throws Throwable {
        IOperator operator = opClass.newInstance();
        OutputStreamDeclarer declarer = new OutputStreamDeclarer(topologyName, taskName);
        operator.declareOutputStream(declarer);
        Map<String, DynamicSchema> outboundSchemaMap = declarer.getOutputStreamSchemas();
        Transaction txn = zkConn.transaction();
        for (Map.Entry<String, DynamicSchema> e : outboundSchemaMap.entrySet()) {
            String outboundPath = "/stream/" + e.getKey();
            zkConn.create(outboundPath, null);
            txn.setData(outboundPath, e.getValue().toByteArray(), -1);
        }
        if (!outboundSchemaMap.isEmpty()) {
            txn.commit();
        }
        return outboundSchemaMap;
    }

    private void registerInboundStream(int port) {
        String inboundPath = "/stream/" + inbound;
        zkConn.create(inboundPath, null);
        String ip = null;
        try {
            ip = SharedUtil.getIp();
        } catch (IOException e) {
            log.error("Cannot get host IP: " + e.toString());
            System.exit(-1);
        }
        zkConn.create(inboundPath + "/" + ip + ":" + port, null, CreateMode.EPHEMERAL);
    }

    private void decodeTaskFullName(String taskFullName) {
        // [0] => topologyName
        // [1] => taskName
        // [2] => processIndex
        // [3] => threadNum
        // [4] => inboundStr
        // [5] => outboundStr
        String[] taskInfo = taskFullName.split("#", -1);
        topologyName = taskInfo[0];
        taskName = taskInfo[1];
        threadNum = Integer.parseInt(taskInfo[3]);
        // Only one inbound stream is allowed for every bolt
        inbound = taskInfo[4].split(";")[0];
    }

    private int startGrpcServer() throws IOException {
        server = ServerBuilder.forPort(0)
                .intercept(new ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                            ServerCall<ReqT, RespT> call, Metadata headers,
                            ServerCallHandler<ReqT, RespT> next) {
                        call.setCompression("gzip");
                        return next.startCall(call, headers);
                    }
                })
                .addService(messageReceiver)
                .build()
                .start();
        log.info("Server started, listening on tcp port " + server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                WorkerProcessServer.this.stop();
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
            cleanUpSingletonResources();
            log.info("Server shut down");
        }));
        return server.getPort();
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
