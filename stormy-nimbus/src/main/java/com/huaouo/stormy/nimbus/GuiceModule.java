// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus;

import com.google.inject.AbstractModule;

import java.util.Properties;

public class GuiceModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Properties.class).toProvider(PropertiesProvider.class).asEagerSingleton();
    }
}
