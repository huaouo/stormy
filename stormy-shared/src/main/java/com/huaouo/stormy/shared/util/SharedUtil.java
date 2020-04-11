// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.shared.util;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.shared.GuiceModule;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

public class SharedUtil {

    public static String getIp() throws IOException {
        Injector injector = Guice.createInjector(new GuiceModule());
        Properties appProp = injector.getInstance(Properties.class);

        String zkConnStr = appProp.getProperty("stormy.zookeeper.connect_string");
        String[] zkAddrList = zkConnStr.split(",");
        for (String zkAddr : zkAddrList) {
            String[] zkHostAndIp;
            if (zkAddr.contains(":")) {
                zkHostAndIp = zkAddr.split(":");
            } else {
                zkHostAndIp = new String[2];
                zkHostAndIp[0] = zkAddr;
                zkHostAndIp[1] = "2181";
            }

            try (Socket socket = new Socket(zkHostAndIp[0], Integer.parseInt(zkHostAndIp[1]))) {
                return socket.getLocalAddress().getHostAddress();
            } catch (IOException | NumberFormatException ignored) {
            }
        }
        throw new IOException("Unable to get host IP due to fail to connect to any of ZooKeeper server");
    }
}
