// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.service;

import com.huaouo.stormy.wrapper.ZooKeeperConnection;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Singleton
public class ZooKeeperService {

    private ZooKeeperConnection zkConn;

    @Inject
    public ZooKeeperService(ZooKeeperConnection zkConn) {
        this.zkConn = zkConn;
        init();
    }

    public void init() {
        zkConn.createIfNotExistsSync("/master", null);
        zkConn.createIfNotExistsSync("/master/topology", null);
        zkConn.createIfNotExistsSync("/workers", null);
        zkConn.createIfNotExistsSync("/workers/registered", null);
        zkConn.createIfNotExistsSync("/workers/available", null);
    }

    public boolean topologyExists(String topologyName) {
        return zkConn.exists("/master/topology/" + topologyName);
    }

    public void registerTopology(String topologyName) {
        zkConn.createIfNotExistsSync("/master/topology/" + topologyName, "RUN");
    }

    public void deleteTopology(String topologyName) {
        zkConn.deleteRecursiveSync(topologyName);
    }

    public void stopTopology(String topologyName) {
        zkConn.setSync("/master/topology/" + topologyName, "STOP");
    }

    public Map<String, String> getRunningTopologies() {
        List<String> topologyNames = zkConn.getChildrenSync("/master/topology");
        Map<String, String> result = new HashMap<>();
        for (String name : topologyNames) {
            result.put(name, zkConn.getSync("/master/topology/" + name));
        }
        return result;
    }
}
