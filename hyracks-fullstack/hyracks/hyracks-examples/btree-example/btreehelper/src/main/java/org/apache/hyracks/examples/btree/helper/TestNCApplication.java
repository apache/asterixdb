/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.examples.btree.helper;

import org.apache.hyracks.api.application.INCApplication;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.io.IFileDeviceResolver;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.logging.log4j.Level;

public class TestNCApplication implements INCApplication {

    private RuntimeContext rCtx;
    private IConfigManager configManager;

    @Override
    public void init(IServiceContext serviceCtx) throws Exception {
        rCtx = new RuntimeContext((INCServiceContext) serviceCtx);
    }

    @Override
    public void start(String[] args) throws Exception {
        // No-op
    }

    @Override
    public void startupCompleted() throws Exception {
        // No-op
    }

    @Override
    public void tasksCompleted(CcId ccs) throws Exception {
        // No-op
    }

    @Override
    public void stop() throws Exception {
        // No-op
    }

    @Override
    public void preStop() throws Exception {
        // No-op
    }

    @Override
    public NodeCapacity getCapacity() {
        return new NodeCapacity(Runtime.getRuntime().maxMemory(), Runtime.getRuntime().availableProcessors() - 1);
    }

    @Override
    public void registerConfig(IConfigManager configManager) {
        this.configManager = configManager;
    }

    @Override
    public RuntimeContext getApplicationContext() {
        return rCtx;
    }

    @Override
    public IFileDeviceResolver getFileDeviceResolver() {
        return null;
    }

    @Override
    public void configureLoggingLevel(Level level) {
        // No-op
    }

    @Override
    public IConfigManager getConfigManager() {
        return configManager;
    }
}
