// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus.controller;

import com.huaouo.stormy.nimbus.service.JarFileService;
import com.huaouo.stormy.nimbus.service.ZooKeeperService;
import com.huaouo.stormy.rpc.ManageTopologyRequest;
import com.huaouo.stormy.rpc.ManageTopologyRequest.RequestType;
import com.huaouo.stormy.rpc.ManageTopologyResponse;
import com.huaouo.stormy.rpc.ManageTopologyGrpc.ManageTopologyImplBase;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

@Slf4j
@Singleton
public class ManageTopologyController extends ManageTopologyImplBase {

    @Inject
    private JarFileService jarService;

    @Inject
    private ZooKeeperService zkService;

    @Override
    public void manageTopology(ManageTopologyRequest request, StreamObserver<ManageTopologyResponse> responseObserver) {
        RequestType requestType = request.getRequestType();
        String topologyName = request.getTopologyName();
        byte[] jarBytes = request.getJarBytes().toByteArray();
        System.out.println("Request Type: " + requestType);
        System.out.println("Topology Name: " + topologyName);
        System.out.println("Data: " + jarBytes);

        ManageTopologyResponse.Builder responseBuilder = ManageTopologyResponse.newBuilder();
        if (!topologyName.matches("[a-zA-Z0-9]+")) {
            responseBuilder.setMessage("Only alphanumeric characters allowed for topologyName");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        switch (requestType) {
            case UNRECOGNIZED:
                responseBuilder.setMessage("Unrecognized request type");
                break;
            case START_TOPOLOGY:
                if (zkService.topologyExists(topologyName)) {
                    responseBuilder.setMessage("Topology exists");
                    break;
                }
                zkService.registerTopology(topologyName);
                try {
                    jarService.writeJarFile(topologyName, jarBytes);
                } catch (IOException e) {
                    log.error(e.getMessage());
                    responseBuilder.setMessage("Internal error, cannot save jar file");
                    break;
                }
                responseBuilder.setMessage("Success");
                break;
            case STOP_TOPOLOGY:
                break;
            case QUERY_RUNNING_TOPOLOGY:
                break;
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
