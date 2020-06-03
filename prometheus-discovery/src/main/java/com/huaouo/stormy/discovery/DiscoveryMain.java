// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.discovery;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.shared.GuiceModule;

public class DiscoveryMain {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Must pass a path of json file to use file-based service discovery");
            System.exit(-1);
        }

        Injector injector = Guice.createInjector(new GuiceModule());
        injector.getInstance(DiscoveryWorker.class).startAndBlock(args[0]);
    }
}
