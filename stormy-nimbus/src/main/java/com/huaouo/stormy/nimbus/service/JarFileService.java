// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus.service;

import org.apache.commons.lang3.RandomStringUtils;

import javax.inject.Singleton;
import java.nio.file.Path;
import java.nio.file.Paths;

@Singleton
public class JarFileService {

    public JarFileService() {
        String tempDir = System.getProperty("java.io.tmpdir");
        Path p = Paths.get(tempDir, "nimbus-" + RandomStringUtils.randomAlphanumeric(5));
    }

    public void saveJarFile() {

    }
}
