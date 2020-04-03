package com.huaouo.stormy.master.service;

import com.huaouo.stormy.provider.ZooKeeperConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

@Slf4j
@Singleton
public class ZooKeeperService {

    private ZooKeeperConnection zkConn;

    @Inject
    public ZooKeeperService(ZooKeeperConnection zkConn) {
        this.zkConn = zkConn;

        zkConn.createSync("/nimbus", null);
        zkConn.createSync("/nimbus/topology", null);
    }

    public String getNimbusId() {
        String nimbusId;
        if (zkConn.exists("/nimbus/id")) {
            nimbusId = zkConn.getSync("/nimbus/id");
        } else {
            nimbusId = RandomStringUtils.randomAlphanumeric(5);
            zkConn.createSync("/nimbus/id", nimbusId);
        }
        return nimbusId;
    }

    public boolean topologyExists(String topologyName) {
        return zkConn.exists("/nimbus/topology/" + topologyName);
    }

    public void registerTopology(String topologyName) {
        zkConn.createSync("/nimbus/topology/" + topologyName, null);
    }
}
