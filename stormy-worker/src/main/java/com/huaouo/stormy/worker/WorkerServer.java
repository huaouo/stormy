// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker;

import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

@Slf4j
@Singleton
public class WorkerServer {

    @Inject
    ZooKeeperConnection zkConn;

    public void start() {
        String ip = null;
        try {
            ip = SharedUtil.getIp();
        } catch (IOException e) {
            log.error("Cannot get host IP: " + e.toString());
            System.exit(-1);
        }

        if (zkConn.exists("/worker/available/" + ip)) {
            log.error("A worker is already running on this node");
            System.exit(-1);
        }
        zkConn.createSync("/worker/registered/" + ip, null);
        zkConn.createSync("/worker/available/" + ip, null, CreateMode.EPHEMERAL);

    }


}
