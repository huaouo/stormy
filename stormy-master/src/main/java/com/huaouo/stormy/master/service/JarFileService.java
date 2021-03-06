// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.service;

import com.huaouo.stormy.shared.service.BaseJarFileService;

import javax.inject.Singleton;

@Singleton
public class JarFileService extends BaseJarFileService {

    public JarFileService() {
        super("stormy-master");
    }
}
