// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.topology;

import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.api.topology.EdgeDefinition;
import com.huaouo.stormy.api.topology.NodeDefinition;
import com.huaouo.stormy.api.topology.TopologyDefinition;
import com.huaouo.stormy.api.topology.TopologyException;
import lombok.AllArgsConstructor;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.IntStream;

public class TopologyLoader {

    private static final MethodType defineTopologyType =
            MethodType.methodType(TopologyDefinition.class);
    private static final MethodType declareOutputStreamType =
            MethodType.methodType(void.class, OutputStreamDeclarer.class);
    private static final MethodHandles.Lookup lookup = MethodHandles.lookup();

    private Map<String, NodeDefinition> nodes;
    private Map<String, List<EdgeDefinition>> graph;

    public ComputationGraph load(URL jarLocalUrl) throws Throwable {
        URL[] url = {jarLocalUrl};
        try (URLClassLoader loader = URLClassLoader.newInstance(url)) {
            loadTopologyDefinition(loader, jarLocalUrl);
            String spoutId = validateTopology(loader);
            // nodeId => TaskDefinition
            Map<String, TaskDefinition> tasks = detectCycleAndConnectivity(spoutId);
            return new ComputationGraph(spoutId, tasks, augmentGraph());
        }
    }

    // convert a graph like A -> B(with 2 processes) -> C to A#0 -> B#0 -> C#0 plus A#0 -> B#1 -> C#0
    private Map<TaskInstance, Set<TaskInstance>> augmentGraph() {
        // instances associated to nodeId
        Map<String, Set<TaskInstance>> instanceMap = new HashMap<>();
        Map<TaskInstance, Set<TaskInstance>> retGraph = new HashMap<>();
        List<String> sortedNodes = getReverseTopologicalOrder();
        for (String n : sortedNodes) {
            int processNum = nodes.get(n).getProcessNum();
            Set<TaskInstance> nextInstances = new HashSet<>();
            graph.get(n).stream() //edges
                    .map(EdgeDefinition::getTargetId)
                    .forEach(x -> {
                        if (instanceMap.containsKey(x)) {
                            nextInstances.addAll(instanceMap.get(x));
                        }
                    });
            IntStream.range(0, processNum)
                    .forEach(x -> retGraph.put(new TaskInstance(n, x), nextInstances));
            instanceMap.put(n, nextInstances);
        }

        return retGraph;
    }

    private List<String> getReverseTopologicalOrder() {
        Map<String, Integer> outboundEdgesCount = new HashMap<>();
        Map<String, List<String>> prevNodeMap = new HashMap<>();
        Queue<String> processQueue = new ArrayDeque<>();
        for (Map.Entry<String, List<EdgeDefinition>> e : graph.entrySet()) {
            if (e.getValue().isEmpty()) {
                processQueue.add(e.getKey());
            } else {
                outboundEdgesCount.put(e.getKey(), e.getValue().size());
            }

            for (EdgeDefinition edge : e.getValue()) {
                if (!prevNodeMap.containsKey(edge.getTargetId())) {
                    prevNodeMap.put(edge.getTargetId(), new ArrayList<>());
                }
                prevNodeMap.get(edge.getTargetId()).add(e.getKey());
            }
        }

        // reversed topological sorted
        List<String> sorted = new ArrayList<>();
        while (!processQueue.isEmpty()) {
            String e = processQueue.poll();
            sorted.add(e);
            List<String> prevNodes = prevNodeMap.get(e);
            for (String prevNode : prevNodes) {
                int newValue = outboundEdgesCount.get(prevNode) - 1;
                if (newValue == 0) {
                    processQueue.add(prevNode);
                    outboundEdgesCount.remove(prevNode);
                } else {
                    outboundEdgesCount.put(prevNode, newValue);
                }
            }
        }

        return sorted;
    }

    private void loadTopologyDefinition(URLClassLoader loader, URL jarLocalUrl) throws Throwable {
        Class<?> mainClass;
        try (JarFile jarFile = new JarFile(jarLocalUrl.getFile())) {
            String mainClassName = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
            mainClass = loader.loadClass(mainClassName);
            MethodHandle defineTopologyHandle = lookup.findVirtual(mainClass, "defineTopology", defineTopologyType);
            TopologyDefinition topology = (TopologyDefinition) defineTopologyHandle.invoke(mainClass.newInstance());
            nodes = topology.getNodes();
            graph = topology.getGraph();
        }
    }

    private String validateTopology(URLClassLoader loader) throws Throwable {
        String spoutId = null;
        for (Map.Entry<String, List<EdgeDefinition>> e : graph.entrySet()) {
            String sourceId = e.getKey();

            Class<?> sourceClass = loader.loadClass(nodes.get(sourceId).getClassName());
            if (ISpout.class.isAssignableFrom(sourceClass)) {
                spoutId = sourceId;
            }
            MethodHandle declareOutputStreamHandle = lookup.findVirtual(sourceClass,
                    "declareOutputStream", declareOutputStreamType);
            OutputStreamDeclarer declarer = new OutputStreamDeclarer(sourceId);
            declareOutputStreamHandle.invoke(sourceClass.newInstance(), declarer);
            Set<String> schemaNames = declarer.getOutputStreamSchemas().keySet();
            if (schemaNames.size() != e.getValue().size()) {
                throw new Exception("Number of schemas defined in '" + sourceId
                        + "' doesn't match its output stream numbers");
            }

            Set<String> targetIds = new HashSet<>();
            Set<String> streamIds = new HashSet<>();
            for (EdgeDefinition d : e.getValue()) {
                String targetId = d.getTargetId();
                if (targetIds.contains(targetId)) {
                    throw new TopologyException("TargetId '" + targetId
                            + "' mentioned more than once in streams from '" + sourceId + "'");
                }
                targetIds.add(targetId);

                String streamId = d.getStreamId();
                String realStreamId = streamId.split("-")[1];
                if (streamIds.contains(streamId)) {
                    throw new TopologyException("StreamId '" + realStreamId
                            + "' mentioned more than once in streams from '" + sourceId + "'");
                }
                streamIds.add(streamId);

                if (!nodes.containsKey(sourceId)) {
                    throw new TopologyException("Unknown sourceId in stream declaration: " + sourceId);
                }
                if (!nodes.containsKey(targetId)) {
                    throw new TopologyException("Unknown targetId in stream declaration: " + targetId);
                }

                if (!schemaNames.contains(streamId)) {
                    throw new TopologyException("Stream with id '" + realStreamId
                            + "' doesn't exist in source node with id '" + sourceId + "'");
                }
            }
        }
        return spoutId;
    }

    @AllArgsConstructor
    private static class DfsState {
        private String nodeId;
        private int edgeIndex;

        public DfsState nextEdge() {
            return new DfsState(nodeId, edgeIndex + 1);
        }
    }

    // Refer to CLRS, returns a Map, which maps nodeId => TaskDefinition
    private Map<String, TaskDefinition> detectCycleAndConnectivity(String spoutId) throws TopologyException {
        Map<String, TaskDefinition> grayNodes = new HashMap<>();
        Map<String, TaskDefinition> blackNodes = new HashMap<>();
        grayNodes.put(spoutId, new TaskDefinition(nodes.get(spoutId), graph.get(spoutId)));
        Deque<DfsState> states = new ArrayDeque<>();
        states.push(new DfsState(spoutId, 0));

        while (!states.isEmpty()) {
            DfsState s = states.pop();
            List<EdgeDefinition> edges = graph.get(s.nodeId);
            if (s.edgeIndex >= edges.size()) {
                TaskDefinition t = grayNodes.get(s.nodeId);
                grayNodes.remove(s.nodeId);
                blackNodes.put(s.nodeId, t);
                continue;
            } else {
                states.push(s.nextEdge());
            }
            String thisNode = edges.get(s.edgeIndex).getTargetId();
            if (grayNodes.containsKey(thisNode)) {
                throw new TopologyException("Cycle detected in topology");
            }
            if (blackNodes.containsKey(thisNode)) {
                continue;
            }

            if (graph.containsKey(thisNode)) {
                grayNodes.put(thisNode, new TaskDefinition(nodes.get(thisNode), graph.get(thisNode)));
                states.push(new DfsState(thisNode, 0));
            } else {
                blackNodes.put(thisNode, new TaskDefinition(nodes.get(thisNode), new ArrayList<>()));
            }
        }

        if (blackNodes.size() != nodes.size()) {
            throw new TopologyException(
                    "Some nodes aren't connected with spout directly or indirectly");
        }

        for (List<EdgeDefinition> l : graph.values()) {
            for (EdgeDefinition e : l) {
                blackNodes.get(e.getTargetId()).addInboundStream(e.getStreamId());
            }
        }
        return blackNodes;
    }
}
