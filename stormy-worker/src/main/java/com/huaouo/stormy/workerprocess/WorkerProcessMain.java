// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.shared.GuiceModule;

public class WorkerProcessMain {

    public static void main(String[] args) throws Throwable {
        Injector injector = Guice.createInjector(new GuiceModule());
        WorkerProcessServer server = injector.getInstance(WorkerProcessServer.class);
        server.start(args[0], args[1]);
        server.blockUntilShutdown();
    }
}
