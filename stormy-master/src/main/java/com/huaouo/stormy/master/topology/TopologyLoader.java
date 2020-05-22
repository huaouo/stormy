// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.topology;

import com.google.common.collect.Lists;
import com.huaouo.stormy.api.IOperator;
import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.ITopology;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.api.topology.EdgeDefinition;
import com.huaouo.stormy.api.topology.NodeDefinition;
import com.huaouo.stormy.api.topology.TopologyDefinition;
import com.huaouo.stormy.api.topology.TopologyException;
import lombok.AllArgsConstructor;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TopologyLoader {

    private Map<String, NodeDefinition> nodes;
    private Map<String, List<EdgeDefinition>> graph;

    public ComputationGraph load(String topologyName, URL jarLocalUrl) throws Throwable {
        URL[] url = {jarLocalUrl};
        try (URLClassLoader loader = URLClassLoader.newInstance(url)) {
            loadTopologyDefinition(topologyName, loader, jarLocalUrl);
            validateTopology(topologyName, loader);
            String spoutId = getSpoutId(loader);
            // nodeId => TaskDefinition
            Map<String, TaskDefinition> tasks = detectCycleAndConnectivity(spoutId);
            ComputationGraph cGraph = new ComputationGraph(tasks, getAssignOrder(spoutId));

            // add acker
            cGraph.getAssignOrder().add("~acker");
            TaskDefinition ackerTask = new TaskDefinition();
            ackerTask.setProcessNum(1);
            ackerTask.setThreadsPerProcess(3);
            ackerTask.setInboundStreamIds(Lists.newArrayList(topologyName + "-~ackerInbound"));
            cGraph.getTasks().put("~acker", ackerTask);
            return cGraph;
        }
    }

    @AllArgsConstructor
    private static class DfsState<T> {
        private final T key;
        private final int edgeIndex;

        public DfsState<T> nextEdge() {
            return new DfsState<>(key, edgeIndex + 1);
        }
    }

    private String getSpoutId(URLClassLoader loader)
            throws ClassNotFoundException, TopologyException {
        for (Map.Entry<String, NodeDefinition> nodeDef : nodes.entrySet()) {
            Class<?> nodeClass = loader.loadClass(nodeDef.getValue().getClassName());
            if (ISpout.class.isAssignableFrom(nodeClass)) {
                return nodeDef.getKey();
            }
        }
        throw new TopologyException("No spout");
    }

    // Assign order of process, tasks with multiple process will appeared multiple
    // times in the returned list. Null will be inserted between two task names if
    // they are not adjacent in the DAG.
    private List<String> getAssignOrder(String spoutId) {
        List<String> retOrder = new ArrayList<>();
        retOrder.add(spoutId);
        Deque<DfsState<TaskInstance>> dfsStack = new ArrayDeque<>();
        dfsStack.push(new DfsState<>(new TaskInstance(spoutId, 0), 0));
        Map<TaskInstance, List<TaskInstance>> augmentedGraph = augmentGraph();

        Set<TaskInstance> visited = new HashSet<>();
        while (!dfsStack.isEmpty()) {
            DfsState<TaskInstance> state = dfsStack.pop();
            List<TaskInstance> edges = augmentedGraph.get(state.key);
            if (state.edgeIndex < edges.size()) {
                dfsStack.push(state.nextEdge());
            } else {
                continue;
            }
            TaskInstance thisInstance = edges.get(state.edgeIndex);
            if (visited.contains(thisInstance)) {
                retOrder.add(null);
                continue;
            }
            visited.add(thisInstance);
            retOrder.add(thisInstance.getNodeId());
            List<TaskInstance> thisInstanceEdges = augmentedGraph.get(thisInstance);
            if (thisInstanceEdges == null) {
                retOrder.add(null);
            } else {
                dfsStack.push(new DfsState<>(thisInstance, 0));
            }
        }

        return retOrder;
    }

    // Convert a graph like A -> B(with 2 processes) -> C to A#0 -> B#0 -> C#0 plus A#0 -> B#1 -> C#0.
    // SpoutId#0 is the only instance ensured for spout.
    private Map<TaskInstance, List<TaskInstance>> augmentGraph() {
        // instances associated to nodeId
        Map<String, Set<TaskInstance>> instanceMap = new HashMap<>();
        Map<TaskInstance, Set<TaskInstance>> retGraph = new HashMap<>();
        List<String> sortedNodes = getReverseTopologicalOrder();
        for (String n : sortedNodes) {
            int processNum = nodes.get(n).getProcessNum();
            Set<TaskInstance> nextInstances = new HashSet<>();
            if (graph.containsKey(n)) { // not end node of DAG
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

            instanceMap.put(n, IntStream.range(0, processNum)
                    .mapToObj(x -> new TaskInstance(n, x))
                    .collect(Collectors.toSet()));
        }

        return retGraph.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> new ArrayList<>(x.getValue())));
    }

    private List<String> getReverseTopologicalOrder() {
        Map<String, Integer> outboundEdgesCount = new HashMap<>();
        Map<String, List<String>> prevNodeMap = new HashMap<>();
        Queue<String> processQueue = new ArrayDeque<>();
        for (String nodeName : nodes.keySet()) {
            if (!graph.containsKey(nodeName)) {
                processQueue.add(nodeName);
            }
        }
        for (Map.Entry<String, List<EdgeDefinition>> e : graph.entrySet()) {
            outboundEdgesCount.put(e.getKey(), e.getValue().size());

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
            if (prevNodeMap.containsKey(e)) {
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
        }

        return sorted;
    }

    private void loadTopologyDefinition(String topologyName, URLClassLoader loader, URL jarLocalUrl) throws Throwable {
        Class<?> mainClass;
        try (JarFile jarFile = new JarFile(jarLocalUrl.getFile())) {
            String mainClassName = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
            mainClass = loader.loadClass(mainClassName);
            TopologyDefinition topology = ((ITopology) mainClass.newInstance()).defineTopology();
            nodes = topology.getNodes();
            graph = topology.getGraph();

            // add "topologyName-" prefix for streamId
            graph.values().forEach(li ->
                    li.forEach(edge -> edge.setStreamId(topologyName + "-" + edge.getStreamId())));
        }
    }

    private void validateTopology(String topologyName, URLClassLoader loader) throws Throwable {
        for (Map.Entry<String, List<EdgeDefinition>> e : graph.entrySet()) {
            String sourceId = e.getKey();

            Class<?> sourceClass = loader.loadClass(nodes.get(sourceId).getClassName());
            OutputStreamDeclarer declarer = new OutputStreamDeclarer(topologyName, sourceId);
            ((IOperator) sourceClass.newInstance()).declareOutputStream(declarer);
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
    }

    // Refer to CLRS, returns a Map, which maps nodeId => TaskDefinition
    private Map<String, TaskDefinition> detectCycleAndConnectivity(String spoutId) throws TopologyException {
        Map<String, TaskDefinition> grayNodes = new HashMap<>();
        Map<String, TaskDefinition> blackNodes = new HashMap<>();
        grayNodes.put(spoutId, new TaskDefinition(nodes.get(spoutId), graph.get(spoutId)));
        Deque<DfsState<String>> states = new ArrayDeque<>();
        states.push(new DfsState<>(spoutId, 0));

        while (!states.isEmpty()) {
            DfsState<String> s = states.pop();
            List<EdgeDefinition> edges = graph.get(s.key);
            if (s.edgeIndex >= edges.size()) {
                TaskDefinition t = grayNodes.get(s.key);
                grayNodes.remove(s.key);
                blackNodes.put(s.key, t);
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
                states.push(new DfsState<>(thisNode, 0));
            } else {
                blackNodes.put(thisNode, new TaskDefinition(nodes.get(thisNode), new ArrayList<>()));
            }
        }

        if (blackNodes.size() != nodes.size()) {
            throw new TopologyException(
                    "Some operators aren't connected with spout directly or indirectly");
        }

        for (List<EdgeDefinition> l : graph.values()) {
            for (EdgeDefinition e : l) {
                TaskDefinition t = blackNodes.get(e.getTargetId());
                t.addInboundStream(e.getStreamId());
                if (t.getInboundStreamIds().size() != 1) {
                    throw new TopologyException("Only one inbound stream is allowed for each operator");
                }
            }
        }
        return blackNodes;
    }
}
