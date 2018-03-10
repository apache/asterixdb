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
package org.apache.hyracks.api.lifecycle;

import static org.apache.hyracks.util.ExitUtil.EC_UNHANDLED_EXCEPTION;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LifeCycleComponentManager implements ILifeCycleComponentManager {

    public static final class Config {
        public static final String DUMP_PATH_KEY = "DUMP_PATH";
    }

    private static final Logger LOGGER = LogManager.getLogger();

    private final List<ILifeCycleComponent> components;
    private boolean stopInitiated;
    private boolean stopped;
    private String dumpPath;
    private boolean configured;

    public LifeCycleComponentManager() {
        components = new ArrayList<>();
        stopInitiated = false;
        configured = false;
        stopped = false;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            LOGGER.log(Level.ERROR, "Uncaught Exception from thread " + t.getName() + ". Calling shutdown hook", e);
        } finally {
            ExitUtil.exit(EC_UNHANDLED_EXCEPTION);
        }
    }

    @Override
    public synchronized void register(ILifeCycleComponent component) {
        components.add(component);
    }

    @Override
    public void startAll() {
        for (ILifeCycleComponent component : components) {
            component.start();
        }
    }

    @Override
    public synchronized void stopAll(boolean dumpState) throws IOException {
        if (stopped) {
            LOGGER.info("Lifecycle management was already stopped");
            return;
        }
        stopped = true;
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Attempting to stop " + this);
        }
        if (stopInitiated) {
            LOGGER.info("Stop already in progress");
            return;
        }
        if (!configured) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Lifecycle management not configured " + this);
            }
            return;
        }

        stopInitiated = true;
        LOGGER.error("Stopping instance");

        FileOutputStream componentDumpStream = null;
        String componentDumpPath = null;
        for (int index = components.size() - 1; index >= 0; index--) {
            ILifeCycleComponent component = components.get(index);
            try {
                if (dumpState) {
                    componentDumpPath = dumpPath + File.separator + component.getClass().getName() + "-coredump";
                    File f = new File(componentDumpPath);
                    File parentDir = new File(f.getParent());
                    if (!parentDir.exists()) {
                        parentDir.mkdirs();
                    }
                    componentDumpStream = new FileOutputStream(f);
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Stopping component instance " + component.getClass().getName() + "; dump state: "
                            + dumpState + ", dump path: " + componentDumpPath);
                }
                component.stop(dumpState, componentDumpStream);
            } catch (Exception e) {
                LOGGER.log(Level.ERROR, "Exception in stopping component " + component.getClass().getName(), e);
            } finally {
                if (componentDumpStream != null) {
                    componentDumpStream.close();
                }
            }
        }
        stopInitiated = false;
        stopped = true;
    }

    @Override
    public void configure(Map<String, String> configuration) {
        dumpPath = configuration.get(Config.DUMP_PATH_KEY);
        if (dumpPath == null) {
            dumpPath = System.getProperty("user.dir");
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("dump path not configured. Using current directory " + dumpPath);
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("LifecycleComponentManager configured " + this);
        }
        configured = true;
    }

    @Override
    public String getDumpPath() {
        return dumpPath;
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        for (int index = components.size() - 1; index >= 0; index--) {
            ILifeCycleComponent component = components.get(index);
            component.dumpState(os);
        }
    }

    public boolean stoppedAll() {
        return stopped;
    }

}
