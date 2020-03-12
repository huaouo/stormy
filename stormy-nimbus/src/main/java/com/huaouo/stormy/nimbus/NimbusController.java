// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus;

import com.huaouo.stormy.rpc.ManageTopologyRequest;
import com.huaouo.stormy.rpc.ManageTopologyResponse;
import com.huaouo.stormy.rpc.NimbusServiceGrpc;
import com.huaouo.stormy.rpc.NimbusServiceGrpc.NimbusServiceImplBase;
import dagger.grpc.server.GrpcService;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;


@GrpcService(grpcClass = NimbusServiceGrpc.class)
public class NimbusController extends NimbusServiceImplBase {

    @Inject
    public NimbusController() {
    }

    @Override
    public void manageTopology(ManageTopologyRequest request, StreamObserver<ManageTopologyResponse> responseObserver) {
        System.out.println("Request Type: " + request.getRequestType());
        System.out.println("Topology Name: " + request.getTopologyName());
        System.out.println("Data: " + request.getJarBytes());

        ManageTopologyResponse.Builder responseBuilder = ManageTopologyResponse.newBuilder();
        switch (request.getRequestType()) {
            case UNRECOGNIZED:
                responseBuilder.setMessage("Unrecognized request type");
                break;
            case START_TOPOLOGY:
                break;
            case STOP_TOPOLOGY:
                break;
            case QUERY_TOPOLOGY_STATUS:
                break;
        }
        responseBuilder.setMessage("Success!");
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
