// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker;

import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;

@Slf4j
@Singleton
public class WorkerServer {

    @Inject
    ZooKeeperConnection zkConn;

    public void startAndBlock() {
        String ip = null;
        try {
            ip = SharedUtil.getIp();
        } catch (IOException e) {
            log.error("Cannot get host IP: " + e.toString());
            System.exit(-1);
        }

        String registeredPath = "/worker/registered/" + ip;
        zkConn.create(registeredPath, "0");
        zkConn.create("/worker/nodeData/" + ip, null);
        if (!zkConn.create("/worker/available/" + ip, null, CreateMode.EPHEMERAL)) {
            log.error("A worker is already running on this node");
            System.exit(-1);
        }

        zkConn.addWatch(registeredPath, e -> {
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                List<String> tasks = zkConn.getChildren(registeredPath);
                for (String t : tasks) {
                    System.out.println(t);
                }
            }
        });

        // Block
        synchronized (this) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
