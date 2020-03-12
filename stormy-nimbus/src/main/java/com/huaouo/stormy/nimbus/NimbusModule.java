// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus;

import com.huaouo.stormy.rpc.NimbusServiceGrpc;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.grpc.server.ForGrpcService;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.Collections;
import java.util.List;

@Module
public abstract class NimbusModule {
    @Binds
    public abstract NimbusControllerServiceDefinition manageTopologyComponent(NimbusComponent manageTopologyComponent);

    @Provides
    @ForGrpcService(NimbusServiceGrpc.class)
    public static List<? extends ServerInterceptor> manageTopologyInterceptors() {
        return Collections.singletonList(new ServerInterceptor() {

            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                call.setCompression("gzip");
                return next.startCall(call, headers);
            }
        });
    }
}
