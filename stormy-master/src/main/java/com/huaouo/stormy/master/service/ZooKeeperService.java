// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.service;

import com.huaouo.stormy.master.topology.ComputationGraph;
import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
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
        String masterAddr = null;
        try {
            masterAddr = SharedUtil.getIp() + ":5000";
        } catch (IOException e) {
            log.error("Cannot get host IP: " + e.toString());
            System.exit(-1);
        }

        zkConn.createSync("/master", masterAddr);
        if (!zkConn.createSync("/master/lock", null, CreateMode.EPHEMERAL)) {
            log.error("A master is already running");
            System.exit(-1);
        }
        if (!masterAddr.equals(zkConn.getSync("/master"))) {
            zkConn.setSync("/master", masterAddr);
        }

        zkConn.createSync("/master/id", "0", CreateMode.EPHEMERAL);
        zkConn.createSync("/master/topology", null);
        zkConn.createSync("/worker", null);
        zkConn.createSync("/worker/registered", null);
        zkConn.createSync("/worker/available", null);
    }

    public boolean topologyExists(String topologyName) {
        return zkConn.exists("/master/topology/" + topologyName);
    }

    public void startTopology(String topologyName, ComputationGraph cGraph) {
        zkConn.createSync("/master/topology/" + topologyName, "run");
    }

    public void deleteTopology(String topologyName) {
        zkConn.deleteRecursiveSync(topologyName);
    }

    public void stopTopology(String topologyName) {
        zkConn.setSync("/master/topology/" + topologyName, "stop");
    }

    public Map<String, String> getRunningTopologies() {
        List<String> topologyNames = zkConn.getChildrenSync("/master/topology");
        Map<String, String> result = new HashMap<>();
        for (String name : topologyNames) {
            result.put(name, zkConn.getSync("/master/topology/" + name));
        }
        return result;
    }

    public long generateId() {
        long id = Long.parseLong(zkConn.getSync("/master/id"));
        String newId = Long.toString(id + 1);
        zkConn.setSync("/master/id", newId);
        return id;
    }
}
