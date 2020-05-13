// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.controller;

import com.google.protobuf.Empty;
import com.huaouo.stormy.rpc.RpcTuple;
import com.huaouo.stormy.rpc.TransmitTupleGrpc.TransmitTupleImplBase;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Singleton
public class TransmitTupleController extends TransmitTupleImplBase {

    private static final Empty EMPTY_MESSAGE = Empty.newBuilder().build();

    private BlockingQueue<byte[]> inboundQueue = new LinkedBlockingQueue<>();

    public BlockingQueue<byte[]> getInboundQueue() {
        return inboundQueue;
    }

    @Override
    public void transmitTuple(RpcTuple request, StreamObserver<Empty> responseObserver) {
        try {
            inboundQueue.put(request.getTupleBytes().toByteArray());
        } catch (InterruptedException e) {
            log.error("Failed to receive tuple: " + e.toString());
        }
        responseObserver.onNext(EMPTY_MESSAGE);
        responseObserver.onCompleted();
    }
}
