/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.api.lifecycle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LifeCycleComponentManager implements ILifeCycleComponentManager {

    public final static LifeCycleComponentManager INSTANCE = new LifeCycleComponentManager();

    public static final class Config {
        public static final String DUMP_PATH_KEY = "DUMP_PATH";
    }

    private static final Logger LOGGER = Logger.getLogger(LifeCycleComponentManager.class.getName());

    private final List<ILifeCycleComponent> components;
    private boolean stopInitiated;
    private String dumpPath;
    private boolean configured;

    private LifeCycleComponentManager() {
        components = new ArrayList<ILifeCycleComponent>();
        stopInitiated = false;
        configured = false;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (LOGGER.isLoggable(Level.SEVERE)) {
            LOGGER.severe("Uncaught Exception from thread " + t.getName() + " message: " + e.getMessage());
            e.printStackTrace();
        }
        try {
            stopAll(true);
        } catch (IOException e1) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Exception in stopping Asterix. " + e1.getMessage());
            }
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
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.severe("Attempting to stop " + this);
        }
        if (stopInitiated) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.severe("Stop already in progress");
            }
            return;
        }
        if (!configured) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Lifecycle management not configured" + this);
            }
            return;
        }

        stopInitiated = true;
        if (LOGGER.isLoggable(Level.SEVERE)) {
            LOGGER.severe("Stopping Asterix instance");
        }

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
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Stopping component instance" + component.getClass().getName() + " dump state "
                            + dumpState + " dump path " + componentDumpPath);
                }
                component.stop(dumpState, componentDumpStream);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Exception in stopping component " + component.getClass().getName() + e.getMessage());
                }
            } finally {
                if (componentDumpStream != null) {
                    componentDumpStream.close();
                }
            }
        }
        stopInitiated = false;

    }

    @Override
    public void configure(Map<String, String> configuration) {
        dumpPath = configuration.get(Config.DUMP_PATH_KEY);
        if (dumpPath == null) {
            dumpPath = System.getProperty("user.dir");
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("dump path not configured. Using current directory " + dumpPath);
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.severe("LifecycleComponentManager configured " + this);
        }
        configured = true;
    }

}
