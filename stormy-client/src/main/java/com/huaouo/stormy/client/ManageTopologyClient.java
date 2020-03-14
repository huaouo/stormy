// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.client;

import com.google.protobuf.ByteString;
import com.huaouo.stormy.rpc.ManageTopologyRequest;
import com.huaouo.stormy.rpc.ManageTopologyRequest.RequestType;
import com.huaouo.stormy.rpc.ManageTopologyResponse;
import com.huaouo.stormy.rpc.ManageTopologyGrpc;
import com.huaouo.stormy.rpc.ManageTopologyGrpc.ManageTopologyBlockingStub;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ManageTopologyClient {

    private final ManageTopologyBlockingStub blockingStub;

    public ManageTopologyClient(Channel channel) {
        blockingStub = ManageTopologyGrpc.newBlockingStub(channel);
    }

    public String manageTopology(RequestType requestType, String topologyName, InputStream jarByteStream)
            throws IOException {
        if (jarByteStream == null) {
            jarByteStream = new ByteArrayInputStream(new byte[0]);
        }
        ManageTopologyRequest clientRequest = ManageTopologyRequest.newBuilder()
                .setRequestType(requestType)
                .setTopologyName(topologyName)
                .setJarBytes(ByteString.readFrom(jarByteStream))
                .build();

        ManageTopologyResponse response;
        try {
            response = blockingStub.withCompression("gzip").manageTopology(clientRequest);
        } catch (StatusRuntimeException e) {
            return e.getMessage();
        }
        return response.getMessage();
    }
}
