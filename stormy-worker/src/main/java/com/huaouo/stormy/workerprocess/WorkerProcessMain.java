package com.huaouo.stormy.workerprocess;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.GuiceModule;

import java.io.IOException;

public class WorkerProcessMain {
    
    public static void main(String[] args) throws IOException {
        Injector injector = Guice.createInjector(new GuiceModule());
        WorkerProcessServer server = injector.getInstance(WorkerProcessServer.class);
        server.start();
    }
}
