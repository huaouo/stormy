// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.util;

import com.huaouo.stormy.api.topology.TopologyDefinition;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.jar.JarFile;

public class MasterUtil {

    public static String formatRunningTopologies(Map<String, String> runningTopologies) {
        StringBuilder builder = new StringBuilder("Running topologies:\n");
        boolean hasTopology = false;
        for (Map.Entry<String, String> e : runningTopologies.entrySet()) {
            if ("run".equals(e.getValue())) {
                builder.append("  ");
                builder.append(e.getKey());
                builder.append("\n");
                hasTopology = true;
            }
        }
        if (!hasTopology) {
            builder.append("  <none>\n");
        }
        return builder.toString();
    }

    private static final MethodType defineTopologyType =
            MethodType.methodType(TopologyDefinition.class);

    public static TopologyDefinition loadTopologyDefinition(URL jarLocalUrl) throws Throwable {
        URL[] url = {jarLocalUrl};
        Class<?> mainClass;
        try (URLClassLoader loader = URLClassLoader.newInstance(url);
             JarFile jarFile = new JarFile(jarLocalUrl.getFile())) {
            String mainClassName = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
            mainClass = loader.loadClass(mainClassName);
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            MethodHandle defineTopologyHandle = lookup.findVirtual(mainClass, "defineTopology", defineTopologyType);
            return (TopologyDefinition) defineTopologyHandle.invoke(mainClass.newInstance());
        }
    }
}
