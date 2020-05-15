// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.thread;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.huaouo.stormy.rpc.RpcTuple;
import com.huaouo.stormy.rpc.TransmitTupleGrpc;
import com.huaouo.stormy.rpc.TransmitTupleGrpc.TransmitTupleStub;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Singleton
public class TransmitTupleClientThread implements Runnable {

    @Inject
    ZooKeeperConnection zkConn;

    private BlockingQueue<ComputedOutput> outboundQueue = new LinkedBlockingQueue<>();
    private Map<String, Map<String, TransmitTupleStub>> clients = new HashMap<>();
    private Map<String, Lock> streamServerLocks = new HashMap<>();
    private Map<String, Iterator<TransmitTupleStub>> iterators = new HashMap<>();

    public void initWithOutbounds(Set<String> outbounds) {
        for (String streamId : outbounds) {
            Map<String, TransmitTupleStub> m = new HashMap<>();
            clients.put(streamId, m);
            iterators.put(streamId, m.values().iterator());
            streamServerLocks.put(streamId, new ReentrantLock());

            zkConn.addWatch("/stream/" + streamId, e -> {
                if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    handleServerChange(streamId);
                }
            });
            handleServerChange(streamId);
        }
    }

    private void handleServerChange(String streamId) {
        try {
            streamServerLocks.get(streamId).lock();
            List<String> currentServers = zkConn.getChildren("/stream/" + streamId);
            Map<String, TransmitTupleStub> clientGroup = clients.get(streamId);
            currentServers.forEach(s -> {
                if (!clientGroup.containsKey(s)) {
                    ManagedChannel channel = ManagedChannelBuilder.forTarget(s)
                            .usePlaintext()
                            .build();
                    clientGroup.put(s, TransmitTupleGrpc.newStub(channel).withCompression("gzip"));
                }
            });
            Iterator<String> iter = clientGroup.keySet().iterator();
            while (iter.hasNext()) {
                String s = iter.next();
                if (!currentServers.contains(s)) {
                    ((ManagedChannel) clientGroup.get(s).getChannel()).shutdown();
                    iter.remove();
                }
            }
            iterators.put(streamId, clientGroup.values().iterator());
        } finally {
            streamServerLocks.get(streamId).unlock();
        }
    }

    public BlockingQueue<ComputedOutput> getOutboundQueue() {
        return outboundQueue;
    }

    @Override
    public void run() {
        if (outboundQueue == null) {
            throw new RuntimeException("Not initialized");
        }

        while (true) {
            Lock lock = null;
            boolean needSleep = false;
            try {
                ComputedOutput output = outboundQueue.take();
                String streamId = output.getStreamId();
                lock = streamServerLocks.get(streamId);
                lock.lock();
                Iterator<TransmitTupleStub> stubIter = iterators.get(streamId);
                if (!stubIter.hasNext()) {
                    Map<String, TransmitTupleStub> clientGroup = clients.get(streamId);
                    if (clientGroup.isEmpty()) {
                        outboundQueue.put(output);
                        needSleep = true;
                        continue;
                    }
                    stubIter = clientGroup.values().iterator();
                }
                TransmitTupleStub stub = stubIter.next();
                iterators.put(streamId, stubIter);
                stub.transmitTuple(RpcTuple.newBuilder()
                        .setTupleBytes(ByteString.copyFrom(output.getBytes()))
                        .build(), new StreamObserver<Empty>() {
                    @Override
                    public void onNext(Empty value) {
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
            } catch (Throwable t) {
                log.error(t.toString());
            } finally {
                if (needSleep) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                }
                if (lock != null) {
                    lock.unlock();
                }
            }
        }
    }
}
