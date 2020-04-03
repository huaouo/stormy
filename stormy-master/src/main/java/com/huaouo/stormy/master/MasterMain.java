// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master;

import java.io.IOException;

public class MasterMain {

    public static void main(String[] args) throws InterruptedException, IOException {
        final MasterServer server = new MasterServer(5000);
        server.start();
        server.blockUntilShutdown();
    }
}
