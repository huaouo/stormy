// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus;

import dagger.Component;
import dagger.grpc.server.NettyServerModule;
import io.grpc.Server;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        NettyServerModule.class,
        NimbusControllerUnscopedGrpcServiceModule.class,
        NimbusModule.class
})
public interface NimbusComponent extends NimbusControllerServiceDefinition {
    Server server();
}
