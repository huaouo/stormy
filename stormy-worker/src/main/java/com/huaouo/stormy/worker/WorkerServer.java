// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker;

import com.huaouo.stormy.rpc.ProvideJarGrpc;
import com.huaouo.stormy.rpc.ProvideJarGrpc.ProvideJarStub;
import com.huaouo.stormy.rpc.ProvideJarRequest;
import com.huaouo.stormy.rpc.ProvideJarResponse;
import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import com.huaouo.stormy.worker.service.JarFileService;
import com.huaouo.stormy.worker.util.WorkerUtil;
import com.huaouo.stormy.workerprocess.WorkerProcessMain;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Singleton
public class WorkerServer {

    @Inject
    private ZooKeeperConnection zkConn;

    @Inject
    private JarFileService jarService;

    private ProvideJarStub grpcStub;
    private Lock zkLock = new ReentrantLock();
    private OperatingSystem os = new SystemInfo().getOperatingSystem();

    private String registeredPath;
    private String acceptedTasksPath;

    public void startAndBlock() {
        String ip = null;
        try {
            ip = SharedUtil.getIp();
        } catch (IOException e) {
            log.error("Cannot get host IP: " + e.toString());
            System.exit(-1);
        }

        registeredPath = "/worker/registered/" + ip;
        String nodeDataPath = "/worker/nodeData/" + ip;
        acceptedTasksPath = nodeDataPath + "/accepted";
        zkConn.create(registeredPath, "0");
        zkConn.create(nodeDataPath, null);
        zkConn.create(acceptedTasksPath, null);
        if (!zkConn.create("/worker/available/" + ip, null, CreateMode.EPHEMERAL)) {
            log.error("A worker is already running on this node");
            System.exit(-1);
        }

        initGrpcClient(zkConn.get("/master"));

        zkConn.addWatch(registeredPath, e -> {
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                handleAssignmentChange();
            }
        });
        handleAssignmentChange();

        // monitor accepted tasks and restart them if necessary
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    zkLock.lock();
                    List<String> acceptedTasks = zkConn.getChildren(acceptedTasksPath);
                    for (String t : acceptedTasks) {
                        String taskPath = acceptedTasksPath + "/" + t;
                        long pid = Long.parseLong(zkConn.get(taskPath));
                        OSProcess p = os.getProcess((int) pid);
                        if (p == null || !p.getName().contains("java")) {
                            // restart process
                            pid = createTaskProcess(t);
                            zkConn.set(taskPath, Long.toString(pid));
                        }
                    }
                } catch (Throwable t) {
                    log.error(t.toString());
                } finally {
                    zkLock.unlock();
                }
            }
        }).start();

        // block
        LockSupport.park();
    }

    private void initGrpcClient(String target) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        grpcStub = ProvideJarGrpc.newStub(channel).withCompression("gzip");
    }

    private void handleAssignmentChange() {
        try {
            zkLock.lock();
            List<String> assignedTasks = zkConn.getChildren(registeredPath);
            List<String> acceptedTasks = zkConn.getChildren(acceptedTasksPath);
            assignedTasks.forEach(t -> {
                if (!acceptedTasks.contains(t)) {
                    acceptTask(t);
                }
            });
            acceptedTasks.forEach(t -> {
                if (!assignedTasks.contains(t)) {
                    removeTask(t);
                }
            });
        } finally {
            zkLock.unlock();
        }
    }

    private void acceptTask(String taskFullName) {
        long pid = createTaskProcess(taskFullName);
        zkConn.create(acceptedTasksPath + "/" + taskFullName, Long.toString(pid));
    }

    private long createTaskProcess(String taskFullName) {
        // [0] => topologyName
        // [1] => taskName
        // [2] => processIndex
        // [3] => threadNum
        // [4] => inboundStr
        // [5] => outboundStr
        String[] taskInfo = taskFullName.split("#", -1);
        String topologyName = taskInfo[0];

        String jarPath = null;
        try {
            jarPath = getJarPath(topologyName);
        } catch (Throwable t) {
            log.error("Failed to get jar: " + t.toString());
            System.exit(-1);
        }

        String classPath = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(WorkerUtil.getJvmPath(), "-cp",
                classPath, WorkerProcessMain.class.getCanonicalName(), jarPath, taskFullName);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        long pid = -1;
        try {
            Process p = pb.start();
            pid = WorkerUtil.getPid(p);
        } catch (Throwable t) {
            log.error("Failed to launch worker process: " + t.toString());
            System.exit(-1);
        }
        return pid;
    }

    private void removeTask(String taskFullName) {
        String removeTaskPath = acceptedTasksPath + "/" + taskFullName;
        long pid = Long.parseLong(zkConn.get(removeTaskPath));
        WorkerUtil.killByPid(pid);
        zkConn.delete(removeTaskPath);
        String topologyName = taskFullName.split("#", -1)[0];
        List<String> acceptedTasks = zkConn.getChildren(acceptedTasksPath);
        if (acceptedTasks.stream().noneMatch(t -> t.startsWith(topologyName + "#"))) {
            try {
                jarService.deleteJarFile(topologyName);
            } catch (Throwable t) {
                log.error("Failed to delete jar file: " + t.toString());
            }
        }
    }

    private String getJarPath(String topologyName) throws InterruptedException {
        while (!jarService.jarFileExists(topologyName)) {
            OutputStream out;
            try {
                out = jarService.getOutputStream(topologyName);
            } catch (IOException e) {
                log.error("Fail to write jar file: " + e.toString());
                continue;
            }
            Runnable closeOutputStream = () -> {
                try {
                    out.close();
                } catch (IOException e) {
                    log.error("Fail to write jar file: " + e.toString());
                }
            };

            ProvideJarRequest request = ProvideJarRequest.newBuilder()
                    .setTopologyName(topologyName)
                    .build();

            CountDownLatch receiveCompleted = new CountDownLatch(1);
            AtomicBoolean success = new AtomicBoolean();
            grpcStub.provideJar(request, new StreamObserver<ProvideJarResponse>() {
                @Override
                public void onNext(ProvideJarResponse value) {
                    try {
                        out.write(value.getJarBytes().toByteArray());
                    } catch (IOException e) {
                        onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Fail to write jar file: " + t.toString());
                    closeOutputStream.run();
                    receiveCompleted.countDown();
                }

                @Override
                public void onCompleted() {
                    success.set(true);
                    closeOutputStream.run();
                    receiveCompleted.countDown();
                }
            });

            receiveCompleted.await();
        }
        return jarService.getJarFilePath(topologyName).toString();
    }
}
