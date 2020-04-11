// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.shared;

import com.google.inject.AbstractModule;
import com.huaouo.stormy.shared.provider.AppPropertiesProvider;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import com.huaouo.stormy.shared.provider.ZooKeeperConnectionProvider;

import javax.inject.Singleton;
import java.util.Properties;

public class GuiceModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Properties.class).toProvider(AppPropertiesProvider.class).asEagerSingleton();

        // The following singleton should be released manually at application termination.
        bind(ZooKeeperConnection.class).toProvider(ZooKeeperConnectionProvider.class).in(Singleton.class);
    }
}
