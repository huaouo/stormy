// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.client;

import com.google.protobuf.ByteString;
import com.huaouo.stormy.rpc.ManageTopologyGrpc;
import com.huaouo.stormy.rpc.ManageTopologyGrpc.ManageTopologyStub;
import com.huaouo.stormy.rpc.ManageTopologyRequest;
import com.huaouo.stormy.rpc.ManageTopologyRequestMetadata;
import com.huaouo.stormy.rpc.ManageTopologyRequestMetadata.RequestType;
import com.huaouo.stormy.rpc.ManageTopologyResponse;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

public class ManageTopologyClient {

    private final ManageTopologyStub grpcStub;

    public ManageTopologyClient(Channel channel) {
        grpcStub = ManageTopologyGrpc.newStub(channel);
    }

    @Data
    @NoArgsConstructor
    private static class StringWrapper {
        private String string;
    }

    // jarByteStream won't be closed here
    public String manageTopology(RequestType requestType, String topologyName, InputStream jarFileStream)
            throws IOException, InterruptedException {
        StreamObserver<ManageTopologyRequest> request;
        StringWrapper message = new StringWrapper();
        CountDownLatch responseReceived = new CountDownLatch(1);
        request = grpcStub.withCompression("gzip").manageTopology(new StreamObserver<ManageTopologyResponse>() {
            @Override
            public void onNext(ManageTopologyResponse value) {
                message.setString(value.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                message.setString("Network error: " + t.toString());
                responseReceived.countDown();
            }

            @Override
            public void onCompleted() {
                responseReceived.countDown();
            }
        });

        ManageTopologyRequestMetadata.Builder metadataBuilder = ManageTopologyRequestMetadata.newBuilder()
                .setRequestType(requestType);
        if (topologyName != null) {
            metadataBuilder.setTopologyName(topologyName);
        }
        ManageTopologyRequest.Builder clientRequestBuilder = ManageTopologyRequest.newBuilder()
                .setMetadata(metadataBuilder.build());
        request.onNext(clientRequestBuilder.build());

        if (jarFileStream != null) {
            int len;
            byte[] buf = new byte[64 * 1024];
            while ((len = jarFileStream.read(buf)) != -1) {
                clientRequestBuilder.clear();
                clientRequestBuilder.setJarBytes(ByteString.copyFrom(buf, 0, len));
                request.onNext(clientRequestBuilder.build());
            }
        }
        request.onCompleted();
        responseReceived.await();
        return message.getString();
    }
}
