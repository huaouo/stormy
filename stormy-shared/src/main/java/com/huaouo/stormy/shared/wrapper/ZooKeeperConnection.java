// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.shared.wrapper;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
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
        try {
            return new String(zk.getData(path, null, null));
        } catch (Throwable ignored) {
            return null;
        }
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

    public boolean createSync(String path, String data) {
        return createSync(path, data, CreateMode.PERSISTENT);
    }

    // return false if failed to create znode
    @SneakyThrows
    public boolean createSync(String path, String data, CreateMode createMode) {
        byte[] dataBytes = null;
        if (data != null) {
            dataBytes = data.getBytes();
        }
        try {
            zk.create(path, dataBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (Throwable ignored) {
            return false;
        }
        return true;
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
