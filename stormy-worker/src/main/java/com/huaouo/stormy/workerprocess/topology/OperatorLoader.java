// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.topology;

import com.huaouo.stormy.api.IOperator;
import com.huaouo.stormy.api.ITopology;
import com.huaouo.stormy.api.topology.NodeDefinition;
import com.huaouo.stormy.api.topology.TopologyDefinition;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

@Slf4j
public class OperatorLoader {

    @SuppressWarnings("unchecked")
    public Class<? extends IOperator> load(URL jarLocalUrl, String taskName) throws Throwable {
        URL[] url = {jarLocalUrl};
        try (URLClassLoader loader = URLClassLoader.newInstance(url);
             JarFile jarFile = new JarFile(jarLocalUrl.getFile())) {
            loadAllClasses(jarFile.entries(), loader);
            String mainClassName = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
            Class<?> mainClass = loader.loadClass(mainClassName);
            TopologyDefinition topology = ((ITopology) mainClass.newInstance()).defineTopology();
            Map<String, NodeDefinition> nodes = topology.getNodes();

            String operatorClassName = nodes.get(taskName).getClassName();
            return (Class<? extends IOperator>) loader.loadClass(operatorClassName);
        }
    }

    private void loadAllClasses(Enumeration<JarEntry> jarEntries, URLClassLoader cl) throws ClassNotFoundException {
        while (jarEntries.hasMoreElements()) {
            JarEntry je = jarEntries.nextElement();
            if (je.isDirectory() || !je.getName().endsWith(".class")) {
                continue;
            }
            // -6 because of ".class"
            String className = je.getName().substring(0, je.getName().length() - 6);
            className = className.replace('/', '.');
            cl.loadClass(className);
        }
    }
}
