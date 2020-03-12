// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus;

import java.io.IOException;

public class NimbusMain {

    public static void main(String[] args) throws InterruptedException, IOException {
        final NimbusServer server = new NimbusServer(5000);
        server.start();
        server.blockUntilShutdown();
    }
}
