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
package org.apache.asterix.metadata;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodically recycle temporary datasets.
 *
 * @author yingyib
 */
public class GarbageCollector implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(GarbageCollector.class.getName());

    private static final long CLEANUP_PERIOD = 3600L * 24;

    static {
        // Starts the garbage collector thread which
        // should always be running.
        Thread gcThread = new Thread(new GarbageCollector(), "Metadata GC");
        gcThread.setDaemon(true);
        gcThread.start();
    }

    @Override
    @SuppressWarnings("squid:S2142") // rethrow or interrupt thread on InterruptedException
    public void run() {
        LOGGER.info("Starting Metadata GC");
        while (true) {
            try {
                synchronized (this) {
                    this.wait(CLEANUP_PERIOD);
                }
                MetadataManager.INSTANCE.cleanupTempDatasets();
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Exception cleaning temp datasets", e);
            }
        }
        LOGGER.info("Exiting Metadata GC");
    }

    public static void ensure() {
        // no need to do anything, <clinit> does the work
    }
}
