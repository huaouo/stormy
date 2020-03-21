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

import java.io.IOException;
import java.io.InputStream;

public class ManageTopologyClient {

    private final ManageTopologyBlockingStub blockingStub;

    public ManageTopologyClient(Channel channel) {
        blockingStub = ManageTopologyGrpc.newBlockingStub(channel);
    }

    // jarByteStream won't be closed here
    public String manageTopology(RequestType requestType, String topologyName, InputStream jarFileStream)
            throws IOException {
        ManageTopologyRequest.Builder clientRequestBuilder = ManageTopologyRequest.newBuilder()
                .setRequestType(requestType)
                .setTopologyName(topologyName);
        if (jarFileStream != null) {
            clientRequestBuilder.setJarBytes(ByteString.readFrom(jarFileStream));
        }

        ManageTopologyResponse response;
        try {
            response = blockingStub.withCompression("gzip").manageTopology(clientRequestBuilder.build());
        } catch (StatusRuntimeException e) {
            return e.getMessage();
        }
        return response.getMessage();
    }
}
