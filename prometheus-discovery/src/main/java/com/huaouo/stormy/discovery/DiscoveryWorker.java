// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@Singleton
public class DiscoveryWorker {

    @Inject
    private ZooKeeperConnection zkConn;

    private final List<TargetItem> targetJsonObject = new ArrayList<>();
    private final ObjectMapper mapper = new ObjectMapper();

    public DiscoveryWorker() {
        targetJsonObject.add(new TargetItem());
    }

    public void startAndBlock(String jsonPath) {
        zkConn.addWatch("/exporter", e -> {
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                handleExporterChange(jsonPath);
            }
        });
        handleExporterChange(jsonPath);
        LockSupport.park();
    }

    private synchronized void handleExporterChange(String jsonPath) {
        try {
            List<String> exporters = zkConn.getChildren("/exporter");
            targetJsonObject.get(0).setTargets(exporters);
            mapper.writeValue(new File(jsonPath), targetJsonObject);
        } catch (Throwable t) {
            log.error("Failed to update target json: " + t.getMessage());
        }
    }
}
