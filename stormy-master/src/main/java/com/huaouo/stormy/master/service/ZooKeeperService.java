// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.service;

import com.huaouo.stormy.master.topology.ComputationGraph;
import com.huaouo.stormy.master.topology.TaskDefinition;
import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Transaction;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

@Slf4j
@Singleton
public class ZooKeeperService {

    private final ZooKeeperConnection zkConn;

    @Inject
    public ZooKeeperService(ZooKeeperConnection zkConn) {
        this.zkConn = zkConn;
        init();
    }

    public void init() {
        String masterAddr = null;
        try {
            masterAddr = SharedUtil.getIp() + ":6000";
        } catch (IOException e) {
            log.error("Cannot get host IP: " + e.toString());
            System.exit(-1);
        }

        zkConn.create("/master", masterAddr);
        if (!zkConn.create("/master/lock", null, CreateMode.EPHEMERAL)) {
            log.error("A master is already running");
            System.exit(-1);
        }
        if (!masterAddr.equals(zkConn.get("/master"))) {
            zkConn.set("/master", masterAddr);
        }

        zkConn.create("/master/topology", null);
        zkConn.create("/worker", null);
        // for task assignment, persistent children
        zkConn.create("/worker/registered", null);
        // for worker registration, ephemeral children
        zkConn.create("/worker/available", null);
        // for storing workers' data
        zkConn.create("/worker/nodeData", null);
        zkConn.create("/stream", null);
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

    @SneakyThrows
    public synchronized void startTopology(String topologyName, ComputationGraph cGraph) {
        List<String> availableWorkers = zkConn.getChildren("/worker/available");
        if (availableWorkers.isEmpty()) {
            throw new RuntimeException("No workers available");
        }
        AtomicInteger totalAssignedThreads = new AtomicInteger(cGraph.getTotalThreads());
        Queue<LoadInfo> loadInfos = new PriorityQueue<>(Comparator.comparingInt(LoadInfo::getThreads));
        availableWorkers.stream()
                .map(x -> {
                    String load = zkConn.get("/worker/registered/" + x);
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

        Map<String, Integer> encodeHelper = new HashMap<>();
        Function<String, String> encodeAssignment = taskName -> {
            TaskDefinition taskDef = tasks.get(taskName);
            int threadNum = taskDef.getThreadsPerProcess();
            String inboundStr = taskDef.getInboundStreamIds().stream()
                    .reduce("", (l, r) -> r + ";" + l);
            String outboundStr = taskDef.getOutboundStreamIds().stream()
                    .reduce("", (l, r) -> r + ";" + l);
            if (!encodeHelper.containsKey(taskName)) {
                encodeHelper.put(taskName, -1);
            }
            int processIndex = encodeHelper.get(taskName) + 1;
            encodeHelper.put(taskName, processIndex);
            return topologyName + "#" + taskName + "#" + processIndex + "#" + threadNum +
                    "#" + inboundStr + "#" + outboundStr;
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

        Transaction txn = zkConn.transaction();
        while (!loadInfos.isEmpty()) {
            LoadInfo load = loadInfos.poll();
            String workerPath = "/worker/registered/" + load.getWorkerId();
            txn.setData(workerPath, Integer.toString(load.getThreads()).getBytes(), -1);
            for (String assignment : load.getNewAssignments()) {
                txn.create(workerPath + "/" + assignment, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
        txn.create("/master/topology/" + topologyName, "run".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.commit();
    }

    public void stopTopology(String topologyName) {
        String topologyPath = "/master/topology/" + topologyName;
        zkConn.set(topologyPath, "stop");
        String registeredPath = "/worker/registered";
        List<String> registeredWorkers = zkConn.getChildren(registeredPath);

        // delete assigned tasks
        for (String worker : registeredWorkers) {
            String workerPath = registeredPath + "/" + worker;
            int threadNum = Integer.parseInt(zkConn.get(workerPath));
            List<String> assignedTasks = zkConn.getChildren(workerPath);
            for (String task : assignedTasks) {
                if (task.startsWith(topologyName + "#")) {
                    // [0] => topologyName
                    // [1] => taskName
                    // [2] => processIndex
                    // [3] => threadNum
                    // [4] => inboundStr
                    // [5] => outboundStr
                    threadNum -= Integer.parseInt(task.split("#", -1)[3]);
                    zkConn.delete(workerPath + "/" + task);
                }
            }
            zkConn.set(workerPath, Integer.toString(threadNum));
        }

        // delete streams
        String streamPath = "/stream";
        List<String> streams = zkConn.getChildren(streamPath);
        for (String stream : streams) {
            if (stream.startsWith(topologyName + "-")) {
                zkConn.deleteRecursive(streamPath + "/" + stream);
            }
        }

        zkConn.delete(topologyPath);
    }

    public Map<String, String> getRunningTopologies() {
        List<String> topologyNames = zkConn.getChildren("/master/topology");
        Map<String, String> result = new HashMap<>();
        for (String name : topologyNames) {
            result.put(name, zkConn.get("/master/topology/" + name));
        }
        return result;
    }
}
