// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.controller;

import com.huaouo.stormy.master.service.JarFileService;
import com.huaouo.stormy.master.service.ZooKeeperService;
import com.huaouo.stormy.master.util.MasterUtil;
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
                    log.error(e.toString());
                    zkService.deleteTopology(topologyName);
                    responseBuilder.setMessage("Internal error, cannot save jar file");
                    break;
                }
                // TODO: start topology
                responseBuilder.setMessage("Success");
                break;
            case STOP_TOPOLOGY:
                zkService.stopTopology(topologyName);
                // TODO: add stop hook, delete zk entry and jar file
                responseBuilder.setMessage("Success");
                break;
            case QUERY_RUNNING_TOPOLOGY:
                String message = MasterUtil.formatRunningTopologies(zkService.getRunningTopologies());
                responseBuilder.setMessage(message);
                break;
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}