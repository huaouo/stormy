// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.controller;

import com.google.protobuf.ByteString;
import com.huaouo.stormy.master.service.JarFileService;
import com.huaouo.stormy.rpc.ProvideJarGrpc.ProvideJarImplBase;
import com.huaouo.stormy.rpc.ProvideJarRequest;
import com.huaouo.stormy.rpc.ProvideJarResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@Slf4j
@Singleton
public class ProvideJarController extends ProvideJarImplBase {

    @Inject
    private JarFileService jarService;

    @Override
    public void provideJar(ProvideJarRequest request, StreamObserver<ProvideJarResponse> responseObserver) {
        String topologyName = request.getTopologyName();
        ProvideJarResponse.Builder responseBuilder = ProvideJarResponse.newBuilder();
        if (!jarService.jarFileExists(topologyName)) {
            responseBuilder.setMessage("Jar file doesn't exist");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        InputStream jarInputStream = null;
        ByteString jarBytes = null;
        try {
            jarInputStream = jarService.readJarFile(topologyName);
            jarBytes = ByteString.readFrom(jarInputStream);
        } catch (IOException e) {
            log.error(e.toString());
        } finally {
            if (jarInputStream != null) {
                try {
                    jarInputStream.close();
                } catch (IOException e) {
                    log.error(e.toString());
                }
            }
        }
        responseBuilder.setJarBytes(jarBytes);
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
