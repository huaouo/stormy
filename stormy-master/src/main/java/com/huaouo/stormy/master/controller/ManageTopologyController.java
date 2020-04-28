// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.controller;

import com.huaouo.stormy.master.service.JarFileService;
import com.huaouo.stormy.master.service.ZooKeeperService;
import com.huaouo.stormy.master.topology.TaskDefinition;
import com.huaouo.stormy.master.topology.TopologyLoader;
import com.huaouo.stormy.rpc.ManageTopologyGrpc.ManageTopologyImplBase;
import com.huaouo.stormy.rpc.ManageTopologyRequest;
import com.huaouo.stormy.rpc.ManageTopologyRequestMetadata;
import com.huaouo.stormy.rpc.ManageTopologyRequestMetadata.RequestType;
import com.huaouo.stormy.rpc.ManageTopologyResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;

@Slf4j
@Singleton
public class ManageTopologyController extends ManageTopologyImplBase {

    @Inject
    private JarFileService jarService;

    @Inject
    private ZooKeeperService zkService;

    @Override
    public StreamObserver<ManageTopologyRequest> manageTopology(StreamObserver<ManageTopologyResponse> responseObserver) {
        return new StreamObserver<ManageTopologyRequest>() {

            RequestType requestType = RequestType.UNRECOGNIZED;
            String topologyName = null;
            private OutputStream jarFileOutputStream;
            private String message;

            @Override
            public void onNext(ManageTopologyRequest value) {
                if (value.hasMetadata()) {
                    if (requestType == RequestType.UNRECOGNIZED) {
                        ManageTopologyRequestMetadata metadata = value.getMetadata();
                        requestType = metadata.getRequestType();
                        topologyName = metadata.getTopologyName();
                        validateTopologyName();
                        if (message == null) {
                            jarFileOutputStream = getJarFileOutputStream();
                        }
                    }
                } else {
                    if (jarFileOutputStream != null) {
                        try {
                            jarFileOutputStream.write(value.getJarBytes().toByteArray());
                        } catch (IOException e) {
                            log.error(e.toString());
                            if (message == null) {
                                message = "Fail to write jar file: " + e.toString();
                            }
                        }
                    } else if (message == null) {
                        message = "Internal error, unexpected jar file";
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error(t.toString());
                if (message == null) {
                    message = "Network error: " + t.toString();
                }
                closeJarFileOutputStream();
            }

            @Override
            public void onCompleted() {
                closeJarFileOutputStream();
                if (message == null) {
                    processRequest();
                }

                ManageTopologyResponse resp = ManageTopologyResponse.newBuilder()
                        .setMessage(message).build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
            }

            private void closeJarFileOutputStream() {
                if (jarFileOutputStream != null) {
                    try {
                        jarFileOutputStream.close();
                    } catch (IOException e) {
                        log.error(e.toString());
                        if (message == null) {
                            message = "Fail to write jar file: " + e.toString();
                        }
                    }
                }
            }

            private void validateTopologyName() {
                if (!RequestType.QUERY_RUNNING_TOPOLOGY.equals(requestType)
                        && !topologyName.matches("[a-zA-Z0-9]+")) {
                    message = "Only alphanumeric characters allowed for topologyName";
                }
            }

            private OutputStream getJarFileOutputStream() {
                if (requestType == RequestType.START_TOPOLOGY) {
                    try {
                        return jarService.getOutputStream(topologyName);
                    } catch (IOException e) {
                        if (message == null) {
                            message = "Fail to write jar file: " + e.toString();
                        }
                    }
                }
                return null;
            }

            private String formatRunningTopologies(Map<String, String> runningTopologies) {
                StringBuilder builder = new StringBuilder("Running topologies:\n");
                boolean hasTopology = false;
                for (Map.Entry<String, String> e : runningTopologies.entrySet()) {
                    if ("run".equals(e.getValue())) {
                        builder.append("  ");
                        builder.append(e.getKey());
                        builder.append("\n");
                        hasTopology = true;
                    }
                }
                if (!hasTopology) {
                    builder.append("  <none>\n");
                }
                return builder.toString();
            }

            private void processRequest() {
                switch (requestType) {
                    case START_TOPOLOGY:
                        if (zkService.topologyExists(topologyName)) {
                            message = "Topology exists";
                            break;
                        }

                        Map<String, TaskDefinition> tasks;
                        try {
                            URL jarLocalUrl = jarService.getJarFileUrl(topologyName);
                            tasks = new TopologyLoader().load(jarLocalUrl);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            message = "Unable to load topology definition: " + e.toString();
                            break;
                        }

                        zkService.startTopology(topologyName, tasks);
                        // TODO: start topology
                        message = "Success";
                        break;
                    case STOP_TOPOLOGY:
                        zkService.stopTopology(topologyName);
                        // TODO: add stop hook, delete zk entry and jar file
                        message = "Success";
                        break;
                    case QUERY_RUNNING_TOPOLOGY:
                        message = formatRunningTopologies(zkService.getRunningTopologies());
                        break;
                }
            }
        };
    }
}
