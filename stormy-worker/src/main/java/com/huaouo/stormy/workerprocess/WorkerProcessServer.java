// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.Descriptors;
import com.huaouo.stormy.api.IOperator;
import com.huaouo.stormy.api.stream.DynamicSchema;
import com.huaouo.stormy.api.stream.FieldType;
import com.huaouo.stormy.api.stream.MessageDefinition;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.shared.GuiceModule;
import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import com.huaouo.stormy.workerprocess.acker.Acker;
import com.huaouo.stormy.workerprocess.controller.TransmitTupleController;
import com.huaouo.stormy.workerprocess.metrics.MetricsServer;
import com.huaouo.stormy.workerprocess.thread.ComputedOutput;
import com.huaouo.stormy.workerprocess.thread.ComputeThread;
import com.huaouo.stormy.workerprocess.thread.TransmitTupleClientThread;
import com.huaouo.stormy.workerprocess.topology.OperatorLoader;
import io.grpc.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheus.PrometheusMeterRegistry;
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
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Singleton
public class WorkerProcessServer {

    @Inject
    private TransmitTupleController messageReceiver;

    @Inject
    private TransmitTupleClientThread messageSender;

    @Inject
    private ZooKeeperConnection zkConn;

    @Inject
    private MetricsServer metricsServer;

    @Inject
    PrometheusMeterRegistry prometheusRegistry;

    private final ExecutorService threadPool = Executors.newCachedThreadPool(r -> {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setDaemon(true);
        return t;
    });

    private Server server;

    private int threadNum;
    private String topologyName;
    private String taskName;
    private String processIndex;
    private String inbound;

    public void start(String jarPath, String taskFullName) throws Throwable {
        decodeTaskFullName(taskFullName);
        metricsServer.asyncStart(topologyName, taskName + ":" + processIndex);
        boolean isAcker = "~acker".equals(taskName);

        Class<? extends IOperator> opClass;
        if (isAcker) {
            opClass = Acker.class;
        } else {
            URL jarUrl = Paths.get(jarPath).toUri().toURL();
            opClass = new OperatorLoader().load(jarUrl, taskName);
        }

        // acker schema
        DynamicSchema ackerSchema = null;
        MessageDefinition msgDef = MessageDefinition.newBuilder("TupleData")
                .addField(FieldType.STRING, "_topologyName")
                .addField(FieldType.INT, "_spoutTupleId")
                .addField(FieldType.INT, "_traceId")
                .build();
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.addMessageDefinition(msgDef);
        try {
            ackerSchema = schemaBuilder.build();
        } catch (Descriptors.DescriptorValidationException e) {
            log.error(e.toString());
            System.exit(-1);
        }
        Map<String, DynamicSchema> outboundSchemaMap = registerOutboundSchemas(opClass, ackerSchema);

        BlockingQueue<byte[]> inboundQueue = messageReceiver.getInboundQueue();
        int port = startGrpcServer();
        registerInboundStream(port);

        messageSender.init(outboundSchemaMap.keySet());
        BlockingQueue<ComputedOutput> outboundQueue = messageSender.getOutboundQueue();

        DynamicSchema inboundSchema;
        if ("~acker".equals(taskName)) {
            inboundSchema = ackerSchema;
        } else {
            inboundSchema = blockUntilInboundSchemaAvailable();
        }
        threadPool.submit(messageSender);
        IOperator op = opClass.newInstance();
        if (op instanceof Acker) {
            Counter tupleCount = prometheusRegistry.counter("topology.tuple.count");
            Counter latencySum = prometheusRegistry.counter("topology.latency.sum");
            ((Acker) op).setCounters(tupleCount, latencySum);
        }
        for (int i = 0; i < threadNum; ++i) {
            threadPool.submit(new ComputeThread(taskFullName + i,
                    topologyName, op, inboundSchema, outboundSchemaMap,
                    inboundQueue, outboundQueue, ackerSchema));
        }
        // TODO: add thread monitor as new thread
    }

    @Data
    private static class BytesWrapper {
        private byte[] bytes;
    }

    private DynamicSchema blockUntilInboundSchemaAvailable() throws Throwable {
        if (inbound.isEmpty()) {
            return null;
        }
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

    private Map<String, DynamicSchema> registerOutboundSchemas(Class<? extends IOperator> opClass,
                                                               DynamicSchema ackerSchema) throws Throwable {
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

        String ackerStreamId = topologyName + "-~ackerInbound";
        outboundSchemaMap.put(ackerStreamId, ackerSchema);
        zkConn.create("/stream/" + ackerStreamId, null);
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
        processIndex = taskInfo[2];
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
