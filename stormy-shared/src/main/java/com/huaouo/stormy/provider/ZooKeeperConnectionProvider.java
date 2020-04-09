// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.provider;

import com.huaouo.stormy.wrapper.ZooKeeperConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooKeeper;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ZooKeeperConnectionProvider implements Provider<ZooKeeperConnection> {

    @Inject
    private Properties appProp;

    @Override
    public ZooKeeperConnection get() {
        String connectString = appProp.getProperty("stormy.zookeeper.connect_string");
        int sessionTimeout = Integer.parseInt(appProp.getProperty("stormy.zookeeper.session_timeout"));
        int connectTimeout = Integer.parseInt(appProp.getProperty("stormy.zookeeper.connect_timeout"));

        ZooKeeper zk = null;
        CountDownLatch latch = new CountDownLatch(1);
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, event -> {
                switch (event.getState()) {
                    case SyncConnected:
                        latch.countDown();
                        break;
                    case Expired:
                        log.error("ZooKeeper session expired");
                        break;
                }
            });
            latch.await(connectTimeout, TimeUnit.MILLISECONDS);
        } catch (IOException | InterruptedException e) {
            log.error(e.toString());
            System.exit(-1);
        }
        if (latch.getCount() != 0) {
            log.error("ZooKeeper connecting timeout");
            System.exit(-1);
        }

        return new ZooKeeperConnection(zk);
    }
}
