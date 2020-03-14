// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy;

import com.google.inject.AbstractModule;
import com.huaouo.stormy.provider.AppPropertiesProvider;
import com.huaouo.stormy.provider.RedisConnection;
import com.huaouo.stormy.provider.RedisConnectionProvider;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

import javax.inject.Singleton;
import java.util.Properties;

public class GuiceModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Properties.class).toProvider(AppPropertiesProvider.class).asEagerSingleton();

        // The following singleton should be released manually
        // at application termination in reverse order.
        bind(RedisConnection.class).toProvider(RedisConnectionProvider.class).in(Singleton.class);
    }
}
