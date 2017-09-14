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
package org.apache.hyracks.control.nc;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.application.INCApplication;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.io.IFileDeviceResolver;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;

public class BaseNCApplication implements INCApplication {
    public static final BaseNCApplication INSTANCE = new BaseNCApplication();

    protected BaseNCApplication() {
    }

    @Override
    public void init(IServiceContext serviceCtx) throws Exception {
        // no-op
    }

    @Override
    public void start(String[] args) throws Exception {
        if (args.length > 0) {
            throw new IllegalArgumentException("Unrecognized argument(s): " + Arrays.toString(args));
        }
    }

    @Override
    public void startupCompleted() throws Exception {
        // no-op
    }

    @Override
    public void onRegisterNode() throws Exception {
        // no-op
    }

    @Override
    public void stop() throws Exception {
        // no-op
    }

    @Override
    public void preStop() throws Exception {
        // no-op
    }

    @Override
    public NodeCapacity getCapacity() {
        int allCores = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
        return new NodeCapacity(Runtime.getRuntime().maxMemory(), allCores > 1 ? allCores - 1 : allCores);
    }

    @Override
    public void registerConfig(IConfigManager configManager) {
        configManager.addIniParamOptions(ControllerConfig.Option.CONFIG_FILE, ControllerConfig.Option.CONFIG_FILE_URL);
        configManager.addCmdLineSections(Section.NC, Section.COMMON, Section.LOCALNC);
        configManager.setUsageFilter(getUsageFilter());
        configManager.register(ControllerConfig.Option.class, CCConfig.Option.class, NCConfig.Option.class);
    }

    @Override
    public Object getApplicationContext() {
        return null;
    }

    @Override
    public IFileDeviceResolver getFileDeviceResolver() {
        return new DefaultDeviceResolver();
    }

    protected void configureLoggingLevel(Level level) {
        Logger.getLogger("org.apache.hyracks").setLevel(level);
    }

}
