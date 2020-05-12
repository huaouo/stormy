// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker.util;

import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT;

import java.io.File;
import java.lang.reflect.Field;

public class WorkerUtil {

    private static final String OS = System.getProperty("os.name");
    private static final String JAVA_HOME = System.getProperty("java.home");
    private static final String JVM_PATH;

    static {
        if (OS.startsWith("Win")) {
            JVM_PATH = JAVA_HOME + File.separator + "bin" + File.separator + "java.exe";
        } else {
            JVM_PATH = JAVA_HOME + File.separator + "bin" + File.separator + "java";
        }
    }

    // Refer to: https://stackoverflow.com/a/43426878/8417248
    public static long getPid(Process p) {
        long result = -1;
        try {
            //for windows
            if (p.getClass().getName().equals("java.lang.Win32Process") ||
                    p.getClass().getName().equals("java.lang.ProcessImpl")) {
                Field f = p.getClass().getDeclaredField("handle");
                f.setAccessible(true);
                long pHandle = f.getLong(p);
                Kernel32 kernel = Kernel32.INSTANCE;
                WinNT.HANDLE ntHandle = new WinNT.HANDLE();
                ntHandle.setPointer(Pointer.createConstant(pHandle));
                result = kernel.GetProcessId(ntHandle);
                kernel.CloseHandle(ntHandle);
                f.setAccessible(false);
            }
            //for unix based operating systems
            else if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                result = f.getLong(p);
                f.setAccessible(false);
            }
        } catch (Throwable ignored) {
            result = -1;
        }
        return result;
    }

    public static void killByPid(long pid) {
        if (OS.startsWith("Win")) {
            Kernel32 kernel = Kernel32.INSTANCE;
            // fdwAccess: PROCESS_TERMINATE (0x0001)
            WinNT.HANDLE ntHandle = kernel.OpenProcess(1, false, (int) pid);
            kernel.TerminateProcess(ntHandle, 0);
            kernel.CloseHandle(ntHandle);
        } else { // Unix
            UnixKillWrapper killWrapper = UnixKillWrapper.INSTANCE;
            killWrapper.kill((int) pid, 9);
        }
    }

    public static String getJvmPath() {
        return JVM_PATH;
    }
}
