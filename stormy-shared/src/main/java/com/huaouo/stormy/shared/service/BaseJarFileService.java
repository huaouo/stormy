// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.shared.service;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class BaseJarFileService {
    private final Path dataPath;

    public BaseJarFileService(String dataDir) {

        String tempDir = System.getProperty("java.io.tmpdir");
        dataPath = Paths.get(tempDir, dataDir);
        if (Files.exists(dataPath)) {
            if (!Files.isDirectory(dataPath)) {
                log.error("Cannot create data dir '" + dataDir + "' in '" + tempDir
                        + "', since there's already a file with a same name exists");
                System.exit(-1);
            }
            log.info("Data dir '" + dataPath.toString() + "' used");
        } else {
            try {
                Files.createDirectory(dataPath);
                log.info("Data dir '" + dataPath.toString() + "' created");
            } catch (IOException e) {
                log.error(e.toString());
                System.exit(-1);
            }
        }
    }

    public boolean jarFileExists(String fileBaseName) {
        Path jarPath = dataPath.resolve(fileBaseName + ".jar");
        return Files.exists(jarPath) && Files.isRegularFile(jarPath);
    }

    public void deleteJarFile(String fileBaseName) throws IOException {
        Path jarPath = dataPath.resolve(fileBaseName + ".jar");
        Files.deleteIfExists(jarPath);
    }

    // This method will overwrite jar file if exists.
    public OutputStream getOutputStream(String fileBaseName) throws IOException {
        Path jarPath = dataPath.resolve(fileBaseName + ".jar");
        return Files.newOutputStream(jarPath);
    }

    public InputStream getInputStream(String fileBaseName) throws IOException {
        Path jarPath = dataPath.resolve(fileBaseName + ".jar");
        return Files.newInputStream(jarPath);
    }

    public Path getJarFilePath(String fileBaseName) {
        return dataPath.resolve(fileBaseName + ".jar");
    }

    public URL getJarFileUrl(String fileBaseName) throws MalformedURLException {
        Path jarPath = getJarFilePath(fileBaseName);
        return jarPath.toUri().toURL();
    }
}
