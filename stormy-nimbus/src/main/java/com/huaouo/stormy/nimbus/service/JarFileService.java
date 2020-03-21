// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus.service;

import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Singleton
public class JarFileService {

    private Path nimbusDataPath;

    @Inject
    public JarFileService(ZooKeeperService zkService) {
        String nimbusId = zkService.getNimbusId();

        String tempDir = System.getProperty("java.io.tmpdir");
        nimbusDataPath = Paths.get(tempDir, "nimbus-" + nimbusId);
        if (Files.exists(nimbusDataPath)) {
            if (!Files.isDirectory(nimbusDataPath)) {
                log.error("Cannot create nimbus data dir 'nimbus-" + nimbusId + "' in '" + tempDir
                        + "', since there's already a file with a same name exists");
                System.exit(-1);
            }
            log.info("Nimbus data dir '" + nimbusDataPath.toString() + "' used");
        } else {
            try {
                Files.createDirectory(nimbusDataPath);
                log.info("Nimbus data dir '" + nimbusDataPath.toString() + "' created");
            } catch (IOException e) {
                log.error(e.getMessage());
                System.exit(-1);
            }
        }
    }

    public boolean jarFileExists(String fileBaseName) {
        Path jarPath = nimbusDataPath.resolve(fileBaseName + ".jar");
        return Files.exists(jarPath) && Files.isRegularFile(jarPath);
    }

    public void deleteJarFile(String fileBaseName) throws IOException {
        Path jarPath = nimbusDataPath.resolve(fileBaseName + ".jar");
        Files.deleteIfExists(jarPath);
    }

    // This method will overwrite jar file if exists.
    public void writeJarFile(String fileBaseName, byte[] fileBytes) throws IOException {
        Path jarPath = nimbusDataPath.resolve(fileBaseName + ".jar");
        Files.write(jarPath, fileBytes);
    }

    public InputStream readJarFile(String fileBaseName) throws IOException {
        Path jarPath = nimbusDataPath.resolve(fileBaseName + ".jar");
        return Files.newInputStream(jarPath);
    }
}
