// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus.controller;

import com.huaouo.stormy.nimbus.service.JarFileService;
import com.huaouo.stormy.rpc.ManageTopologyRequest;
import com.huaouo.stormy.rpc.ManageTopologyResponse;
import com.huaouo.stormy.rpc.ManageTopologyGrpc.ManageTopologyImplBase;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ManageTopologyController extends ManageTopologyImplBase {

    @Inject
    JarFileService service;

    @Override
    public void manageTopology(ManageTopologyRequest request, StreamObserver<ManageTopologyResponse> responseObserver) {
        System.out.println("Request Type: " + request.getRequestType());
        System.out.println("Topology Name: " + request.getTopologyName());
        System.out.println("Data: " + request.getJarBytes());

        ManageTopologyResponse.Builder responseBuilder = ManageTopologyResponse.newBuilder();
        if (!request.getTopologyName().matches("[a-zA-Z0-9]+")) {
            responseBuilder.setMessage("Only alphanumeric characters allowed for topologyName");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        switch (request.getRequestType()) {
            case UNRECOGNIZED:
                responseBuilder.setMessage("Unrecognized request type");
                break;
            case START_TOPOLOGY:
                break;
            case STOP_TOPOLOGY:
                break;
            case QUERY_RUNNING_TOPOLOGY:
                break;
        }
        responseBuilder.setMessage("Success!");
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
