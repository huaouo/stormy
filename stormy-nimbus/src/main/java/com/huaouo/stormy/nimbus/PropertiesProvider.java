// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.nimbus;

import lombok.extern.slf4j.Slf4j;

import javax.inject.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

@Slf4j
class PropertiesProvider implements Provider<Properties> {

    @Override
    public Properties get() {
        Path propFilePath = null;
        try {
            propFilePath = Paths.get(
                    getClass().getProtectionDomain().getCodeSource().getLocation().toURI()
            ).getParent().resolve("application.properties");
        } catch (URISyntaxException ignored) {
        }

        InputStream propFileStream = null;
        if (propFilePath != null && Files.exists(propFilePath)) {
            try {
                propFileStream = Files.newInputStream(propFilePath);
            } catch (IOException ignored) {
            }
        }
        if (propFileStream == null) {
            // propFileStream won't be null, since application.properties here is bundled into jar
            propFileStream = getClass().getResourceAsStream("/application.properties");
            if (propFileStream == null) {
                log.error("'application.properties' wasn't bundled into jar");
                System.exit(-1);
            }
        }

        Properties prop = new Properties();
        try {
            prop.load(propFileStream);
        } catch (IOException e) {
            log.error(e.getMessage());
            System.exit(-1);
        } finally {
            try {
                propFileStream.close();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }

        return prop;
    }
}
