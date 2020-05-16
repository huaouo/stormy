// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker.metrics;

import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import javax.inject.Provider;

public class PrometheusMeterRegistryProvider implements Provider<PrometheusMeterRegistry> {

    @Override
    public PrometheusMeterRegistry get() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }
}
