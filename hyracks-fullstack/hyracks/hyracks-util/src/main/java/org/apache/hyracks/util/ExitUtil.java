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
package org.apache.hyracks.util;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("squid:S1147")
public class ExitUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final int EC_NORMAL_TERMINATION = 0;
    public static final int EC_ABNORMAL_TERMINATION = 1;
    public static final int EC_FAILED_TO_STARTUP = 2;
    public static final int EC_FAILED_TO_RECOVER = 3;
    public static final int EC_UNHANDLED_EXCEPTION = 11;
    public static final int EC_IMMEDIATE_HALT = 33;
    public static final int EC_HALT_ABNORMAL_RESERVED_44 = 44;
    public static final int EC_HALT_ABNORMAL_RESERVED_55 = 55;
    public static final int EC_HALT_SHUTDOWN_TIMED_OUT = 66;
    public static final int EC_HALT_WATCHDOG_FAILED = 77;
    public static final int EC_HALT_ABNORMAL_RESERVED_88 = 88;
    public static final int EC_TERMINATE_NC_SERVICE_DIRECTIVE = 99;

    private static final ExitThread exitThread = new ExitThread();
    private static final ShutdownWatchdog watchdogThread = new ShutdownWatchdog();
    private static final MutableLong shutdownHaltDelay = new MutableLong(10 * 60 * 1000L); // 10 minutes default

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(watchdogThread::start));
    }

    private ExitUtil() {
    }

    public static void init() {
        // no-op, the clinit does the work
    }

    public static void exit(int status) {
        synchronized (exitThread) {
            if (exitThread.isAlive()) {
                LOGGER.warn("ignoring duplicate request to exit with status " + status
                        + "; already exiting with status " + exitThread.status + "...");
            } else {
                exitThread.setStatus(status);
                exitThread.start();
            }
        }
    }

    public static void exit(int status, long timeBeforeHalt, TimeUnit timeBeforeHaltUnit) {
        shutdownHaltDelay.setValue(timeBeforeHaltUnit.toMillis(timeBeforeHalt));
        exit(status);
    }

    public static void halt(int status) {
        LOGGER.fatal("JVM halting with status " + status + "; bye!", new Throwable("halt stacktrace"));
        // try to give time for the log to be emitted...
        LogManager.shutdown();
        Runtime.getRuntime().halt(status);
    }

    private static class ShutdownWatchdog extends Thread {

        private ShutdownWatchdog() {
            super("ShutdownWatchdog");
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                exitThread.join(shutdownHaltDelay.getValue()); // 10 min
                if (exitThread.isAlive()) {
                    try {
                        LOGGER.info("Watchdog is angry. Killing shutdown hook");
                    } finally {
                        ExitUtil.halt(EC_HALT_SHUTDOWN_TIMED_OUT);
                    }
                }
            } catch (Throwable th) { // NOSONAR must catch them all
                ExitUtil.halt(EC_HALT_WATCHDOG_FAILED);
            }
        }
    }

    private static class ExitThread extends Thread {
        private int status;

        ExitThread() {
            super("JVM exit thread");
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                LOGGER.info("JVM exiting with status " + status + "; bye!");
            } finally {
                Runtime.getRuntime().exit(status);
            }
        }

        public void setStatus(int status) {
            this.status = status;
        }
    }
}
