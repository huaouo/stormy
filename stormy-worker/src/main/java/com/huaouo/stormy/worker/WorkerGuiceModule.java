// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker;

import com.huaouo.stormy.shared.GuiceModule;
import com.huaouo.stormy.worker.metrics.PrometheusMeterRegistryProvider;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import javax.inject.Singleton;

public class WorkerGuiceModule extends GuiceModule {
    @Override
    protected void configure() {
        super.configure();
        bind(PrometheusMeterRegistry.class)
                .toProvider(PrometheusMeterRegistryProvider.class)
                .in(Singleton.class);
    }
}
