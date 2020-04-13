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

public class TopologyLoader {

    private static final MethodType defineTopologyType =
            MethodType.methodType(TopologyDefinition.class);
    private static final MethodType declareOutputStreamType =
            MethodType.methodType(void.class, OutputStreamDeclarer.class);
    private static final MethodHandles.Lookup lookup = MethodHandles.lookup();

    private Map<String, NodeDefinition> nodes;
    private Map<String, List<EdgeDefinition>> graph;

    // Return value: nodeId => TaskDefinition
    public Map<String, TaskDefinition> load(URL jarLocalUrl) throws Throwable {
        URL[] url = {jarLocalUrl};
        try (URLClassLoader loader = URLClassLoader.newInstance(url)) {
            loadTopologyDefinition(loader, jarLocalUrl);
            String spoutId = validateTopology(loader);
            return detectCycleAndConnectivity(spoutId);
        }
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

    // Refer to CLRS, returns a LinkedHashMap indicates the reversed
    // topological order of nodes, which is nodeId => TaskDefinition
    private Map<String, TaskDefinition> detectCycleAndConnectivity(String spoutId) throws TopologyException {
        Map<String, TaskDefinition> grayNodes = new HashMap<>();
        // Use LinkedHashMap because insertion order matters
        Map<String, TaskDefinition> blackNodes = new LinkedHashMap<>();
        grayNodes.put(spoutId, new TaskDefinition(nodes.get(spoutId), graph.get(spoutId)));
        Deque<DfsState> states = new ArrayDeque<>();
        states.push(new DfsState(spoutId, 0));

        while (!states.isEmpty()) {
            DfsState s = states.pop();
            List<EdgeDefinition> edges = graph.get(s.nodeId);
            if (edges.size() <= s.edgeIndex) {
                TaskDefinition t = grayNodes.get(s.nodeId);
                grayNodes.remove(s.nodeId);
                blackNodes.put(s.nodeId, t);
                continue;
            }
            String thisNode = edges.get(s.edgeIndex).getTargetId();
            if (grayNodes.containsKey(thisNode)) {
                throw new TopologyException("Cycle detected in topology");
            }
            if (s.edgeIndex < edges.size()) {
                states.push(s.nextEdge());
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
