// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.wrapper;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;

import java.util.List;

@Slf4j
public class ZooKeeperConnection {

    private ZooKeeper zk;

    public ZooKeeperConnection(ZooKeeper zk) {
        this.zk = zk;
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            log.error(e.toString());
        }
    }

    // If znode doesn't exist, returns null
    @SneakyThrows
    public String getSync(String path) {
        if (!exists(path)) {
            return null;
        }
        return new String(zk.getData(path, null, null));
    }

    @SneakyThrows
    public void getAsync(String path, Watcher watcher) {
        zk.getData(path, watcher, null);
    }

    @SneakyThrows
    public void setSync(String path, String data) {
        byte[] dataBytes = null;
        if (data != null) {
            dataBytes = data.getBytes();
        }
        zk.setData(path, dataBytes, -1);
    }

    @SneakyThrows
    public boolean exists(String path) {
        return zk.exists(path, null) != null;
    }

    public void createIfNotExistsSync(String path, String data) {
        createIfNotExistsSync(path, data, CreateMode.PERSISTENT);
    }

    // Do nothing if path exists
    @SneakyThrows
    public void createIfNotExistsSync(String path, String data, CreateMode createMode) {
        if (exists(path)) {
            return;
        }

        byte[] dataBytes = null;
        if (data != null) {
            dataBytes = data.getBytes();
        }
        zk.create(path, dataBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    }

    @SneakyThrows
    public List<String> getChildrenSync(String path) {
        return zk.getChildren(path, false);
    }

    @SneakyThrows
    public void deleteSync(String path) {
        zk.delete(path, -1);
    }

    @SneakyThrows
    public void deleteRecursiveSync(String path) {
        PathUtils.validatePath(path);

        List<String> children = getChildrenSync(path);
        if (children.isEmpty()) {
            deleteSync(path);
        } else {
            for (String c : children) {
                deleteRecursiveSync(path + "/" + c);
            }
        }
    }
}
