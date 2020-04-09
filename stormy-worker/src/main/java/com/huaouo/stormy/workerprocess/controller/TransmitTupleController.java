package com.huaouo.stormy.workerprocess.controller;

import com.google.protobuf.Empty;
import com.huaouo.stormy.rpc.RpcTuple;
import com.huaouo.stormy.rpc.TransmitTupleGrpc.TransmitTupleImplBase;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

@Slf4j
@Singleton
public class TransmitTupleController extends TransmitTupleImplBase {

    @Override
    public void transmitTuple(RpcTuple request, StreamObserver<Empty> responseObserver) {
        super.transmitTuple(request, responseObserver);
    }
}
