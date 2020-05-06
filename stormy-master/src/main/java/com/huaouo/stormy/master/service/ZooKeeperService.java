// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.service;

import com.huaouo.stormy.master.topology.ComputationGraph;
import com.huaouo.stormy.master.topology.TaskDefinition;
import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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

    @Data
    private static class LoadInfo {
        private String workerId;
        private int threads;
        private List<String> newAssignments = new ArrayList<>();

        public LoadInfo(String workerId, int threads) {
            this.workerId = workerId;
            this.threads = threads;
        }
    }

    public synchronized void startTopology(String topologyName, ComputationGraph cGraph) {
        List<String> availableWorkers = zkConn.getChildrenSync("/worker/available");
        if (availableWorkers.isEmpty()) {
            throw new RuntimeException("No workers available");
        }
        AtomicInteger totalAssignedThreads = new AtomicInteger(cGraph.getTotalThreads());
        Queue<LoadInfo> loadInfos = new PriorityQueue<>(Comparator.comparingInt(LoadInfo::getThreads));
        availableWorkers.stream()
                .map(x -> {
                    String load = zkConn.getSync("/worker/registered/" + x);
                    if (load != null) {
                        int assignedThreads = Integer.parseInt(load);
                        totalAssignedThreads.addAndGet(assignedThreads);
                        return new LoadInfo(x, assignedThreads);
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .forEach(loadInfos::add);
        double avgThreads = (double) totalAssignedThreads.get() / loadInfos.size();
        List<String> assignOrder = cGraph.getAssignOrder();
        Map<String, TaskDefinition> tasks = cGraph.getTasks();

        Function<String, String> encodeAssignment = taskName -> {
            TaskDefinition taskDef = tasks.get(taskName);
            String inboundStr = taskDef.getInboundStreamIds().stream()
                    .map(x -> topologyName + "-" + x)
                    .reduce("", (l, r) -> r + ";" + l);
            String outboundStr = taskDef.getOutboundStreamIds().stream()
                    .map(x -> topologyName + "-" + x)
                    .reduce("", (l, r) -> r + ";" + l);
            return topologyName + "#" + taskName + "#" + taskDef.getThreadsPerProcess()
                    + "#" + inboundStr + "#" + outboundStr;
        };

        int tmpThreads = 0;
        List<String> tmpInstances = new ArrayList<>();
        for (int i = 0; i < assignOrder.size(); i++) {
            String taskName = assignOrder.get(i);
            if (taskName == null) {
                continue; // ignore null separators
            }
            LoadInfo load = loadInfos.peek();
            if (load == null) {
                throw new RuntimeException("Internal Error: loadInfo.peek() == null");
            }
            int curThreads = tasks.get(taskName).getThreadsPerProcess();
            if (tmpThreads + curThreads + load.getThreads() < avgThreads) {
                tmpThreads += curThreads;
                tmpInstances.add(encodeAssignment.apply(taskName));
            } else {
                double prevDelta = avgThreads - tmpThreads;
                double curDelta = tmpThreads + curThreads - avgThreads;
                if (prevDelta <= curDelta && tmpThreads != 0) {
                    --i;
                } else {
                    tmpThreads += curThreads;
                    tmpInstances.add(encodeAssignment.apply(taskName));
                }
                loadInfos.poll();
                load.threads += tmpThreads;
                load.newAssignments.addAll(tmpInstances);
                loadInfos.add(load);
                tmpThreads = 0;
                tmpInstances.clear();
            }
        }

        while (!loadInfos.isEmpty()) {
            assignTask(loadInfos.poll());
        }

        zkConn.createSync("/master/topology/" + topologyName, "run");
    }

    private void assignTask(LoadInfo load) {
        String workerPath = "/worker/registered/" + load.getWorkerId();
        zkConn.setSync(workerPath, Integer.toString(load.getThreads()));
        for (String assignment : load.getNewAssignments()) {
            zkConn.createSync(workerPath + "/" + assignment, null);
        }
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
