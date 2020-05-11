// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.shared.GuiceModule;

public class WorkerMain {

    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new GuiceModule());
        final WorkerServer server = injector.getInstance(WorkerServer.class);
        server.startAndBlock();
    }
}
