// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.topology;

import com.huaouo.stormy.api.IOperator;
import com.huaouo.stormy.api.ITopology;
import com.huaouo.stormy.api.topology.NodeDefinition;
import com.huaouo.stormy.api.topology.TopologyDefinition;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.jar.JarFile;

public class OperatorLoader {

    @SuppressWarnings("unchecked")
    public Class<? extends IOperator> load(URL jarLocalUrl, String taskName) throws Throwable {
        URL[] url = {jarLocalUrl};
        try (URLClassLoader loader = URLClassLoader.newInstance(url);
             JarFile jarFile = new JarFile(jarLocalUrl.getFile())) {
            String mainClassName = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
            Class<?> mainClass = loader.loadClass(mainClassName);
            TopologyDefinition topology = ((ITopology) mainClass.newInstance()).defineTopology();
            Map<String, NodeDefinition> nodes = topology.getNodes();

            String operatorClassName = nodes.get(taskName).getClassName();
            return (Class<? extends IOperator>) loader.loadClass(operatorClassName);
        }
    }
}
