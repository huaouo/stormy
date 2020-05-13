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

    public Transaction transaction() {
        return zk.transaction();
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            log.error(e.toString());
        }
    }

    // If znode doesn't exist, returns null
    public String get(String path) {
        try {
            return new String(zk.getData(path, null, null));
        } catch (Throwable ignored) {
            return null;
        }
    }

    public byte[] getBytesAndWatch(String path, Watcher watcher) {
        try {
            return zk.getData(path, watcher, null);
        } catch (Throwable ignored) {
            return null;
        }
    }

    @SneakyThrows
    public void set(String path, String data) {
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

    public boolean create(String path, String data) {
        return create(path, data, CreateMode.PERSISTENT);
    }

    // return false if failed to create znode
    @SneakyThrows
    public boolean create(String path, String data, CreateMode createMode) {
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
    public List<String> getChildren(String path) {
        return zk.getChildren(path, false);
    }

    @SneakyThrows
    public void delete(String path) {
        zk.delete(path, -1);
    }

    @SneakyThrows
    public void deleteRecursive(String path) {
        PathUtils.validatePath(path);

        List<String> children = getChildren(path);
        if (children.isEmpty()) {
            delete(path);
        } else {
            for (String c : children) {
                deleteRecursive(path + "/" + c);
            }
        }
    }

    @SneakyThrows
    public void addWatch(String path, Watcher watcher) {
        zk.addWatch(path, watcher, AddWatchMode.PERSISTENT);
    }
}
