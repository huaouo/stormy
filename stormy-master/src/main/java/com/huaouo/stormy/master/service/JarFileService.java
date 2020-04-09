// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.service;

import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Singleton
public class JarFileService {

    private Path masterDataPath;

    public JarFileService() {

        String tempDir = System.getProperty("java.io.tmpdir");
        masterDataPath = Paths.get(tempDir, "stormy-master");
        if (Files.exists(masterDataPath)) {
            if (!Files.isDirectory(masterDataPath)) {
                log.error("Cannot create master data dir 'stormy-master' in '" + tempDir
                        + "', since there's already a file with a same name exists");
                System.exit(-1);
            }
            log.info("Master data dir '" + masterDataPath.toString() + "' used");
        } else {
            try {
                Files.createDirectory(masterDataPath);
                log.info("Master data dir '" + masterDataPath.toString() + "' created");
            } catch (IOException e) {
                log.error(e.toString());
                System.exit(-1);
            }
        }
    }

    public boolean jarFileExists(String fileBaseName) {
        Path jarPath = masterDataPath.resolve(fileBaseName + ".jar");
        return Files.exists(jarPath) && Files.isRegularFile(jarPath);
    }

    public void deleteJarFile(String fileBaseName) throws IOException {
        Path jarPath = masterDataPath.resolve(fileBaseName + ".jar");
        Files.deleteIfExists(jarPath);
    }

    // This method will overwrite jar file if exists.
    public void writeJarFile(String fileBaseName, byte[] fileBytes) throws IOException {
        Path jarPath = masterDataPath.resolve(fileBaseName + ".jar");
        Files.write(jarPath, fileBytes);
    }

    public InputStream readJarFile(String fileBaseName) throws IOException {
        Path jarPath = masterDataPath.resolve(fileBaseName + ".jar");
        return Files.newInputStream(jarPath);
    }

    public URL getJarFileUrl(String fileBaseName) throws MalformedURLException {
        Path jarPath = masterDataPath.resolve(fileBaseName + ".jar");
        return jarPath.toUri().toURL();
    }
}
