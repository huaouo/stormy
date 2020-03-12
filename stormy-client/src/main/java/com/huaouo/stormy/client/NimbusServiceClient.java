// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.client;

import com.google.protobuf.ByteString;
import com.huaouo.stormy.rpc.ClientRequest;
import com.huaouo.stormy.rpc.ClientRequest.RequestType;
import com.huaouo.stormy.rpc.NimbusResponse;
import com.huaouo.stormy.rpc.NimbusServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class NimbusServiceClient {

    private final NimbusServiceGrpc.NimbusServiceBlockingStub blockingStub;

    public NimbusServiceClient(Channel channel) {
        blockingStub = NimbusServiceGrpc.newBlockingStub(channel);
    }

    public String manageTopology(RequestType requestType, String topologyName, InputStream jarByteStream)
            throws IOException {
        if (jarByteStream == null) {
            jarByteStream = new ByteArrayInputStream(new byte[0]);
        }
        ClientRequest clientRequest = ClientRequest.newBuilder()
                .setRequestType(requestType)
                .setTopologyName(topologyName)
                .setJarBytes(ByteString.readFrom(jarByteStream))
                .build();

        NimbusResponse response;
        try {
            response = blockingStub.withCompression("gzip").manageTopology(clientRequest);
        } catch (StatusRuntimeException e) {
            return e.getMessage();
        }
        return response.getMessage();
    }
}
