// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.shared.provider;

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
public class AppPropertiesProvider implements Provider<Properties> {

    @Override
    public Properties get() {
        Path appPropFilePath = null;
        try {
            appPropFilePath = Paths.get(
                    getClass().getProtectionDomain().getCodeSource().getLocation().toURI()
            ).getParent().resolve("application.properties");
        } catch (URISyntaxException ignored) {
        }

        InputStream appPropFileStream = null;
        if (appPropFilePath != null && Files.exists(appPropFilePath)) {
            try {
                appPropFileStream = Files.newInputStream(appPropFilePath);
            } catch (IOException ignored) {
            }
        }
        if (appPropFileStream == null) {
            // appPropFileStream won't be null, since application.properties here is bundled into jar
            appPropFileStream = getClass().getResourceAsStream("/application.properties");
            if (appPropFileStream == null) {
                log.error("'application.properties' wasn't bundled into jar");
                System.exit(-1);
            }
            log.info("Using bundled 'application.properties'");
        } else {
            log.info("Using custom 'application.properties'");
        }

        Properties prop = new Properties();
        try {
            prop.load(appPropFileStream);
        } catch (IOException e) {
            log.error(e.toString());
            System.exit(-1);
        } finally {
            try {
                appPropFileStream.close();
            } catch (IOException e) {
                log.error(e.toString());
            }
        }
        System.setProperties(prop);

        return prop;
    }
}
