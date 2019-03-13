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

import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("squid:S1147")
public class ExitUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final int EC_NORMAL_TERMINATION = 0;
    public static final int EC_ABNORMAL_TERMINATION = 1;
    public static final int EC_FAILED_TO_STARTUP = 2;
    public static final int EC_FAILED_TO_RECOVER = 3;
    public static final int EC_NC_FAILED_TO_ABORT_ALL_PREVIOUS_TASKS = 4;
    public static final int EC_FAILED_TO_PROCESS_UN_INTERRUPTIBLE_REQUEST = 5;
    public static final int EC_FAILED_TO_COMMIT_METADATA_TXN = 6;
    public static final int EC_FAILED_TO_ABORT_METADATA_TXN = 7;
    public static final int EC_INCONSISTENT_METADATA = 8;
    public static final int EC_UNCAUGHT_THROWABLE = 9;
    public static final int EC_UNHANDLED_EXCEPTION = 11;
    public static final int EC_FAILED_TO_DELETE_CORRUPTED_RESOURCES = 12;
    public static final int EC_ERROR_CREATING_RESOURCES = 13;
    public static final int EC_TXN_LOG_FLUSHER_FAILURE = 14;
    public static final int EC_NODE_REGISTRATION_FAILURE = 15;
    public static final int EC_NETWORK_FAILURE = 16;
    public static final int EC_ACTIVE_SUSPEND_FAILURE = 17;
    public static final int EC_ACTIVE_RESUME_FAILURE = 18;
    public static final int EC_NC_FAILED_TO_NOTIFY_TASKS_COMPLETED = 19;
    public static final int EC_FAILED_TO_CANCEL_ACTIVE_START_STOP = 22;
    public static final int EC_IMMEDIATE_HALT = 33;
    public static final int EC_HALT_ABNORMAL_RESERVED_44 = 44;
    public static final int EC_IO_SCHEDULER_FAILED = 55;
    public static final int EC_HALT_SHUTDOWN_TIMED_OUT = 66;
    public static final int EC_HALT_WATCHDOG_FAILED = 77;
    public static final int EC_FLUSH_FAILED = 88;
    public static final int EC_TERMINATE_NC_SERVICE_DIRECTIVE = 99;
    private static final ExitThread exitThread = new ExitThread();
    private static final ShutdownWatchdog watchdogThread = new ShutdownWatchdog();
    private static final MutableLong shutdownHaltDelay = new MutableLong(10 * 60 * 1000L); // 10 minutes default

    static {
        watchdogThread.start();
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
                exitThread.setStatus(status, new Throwable("exit callstack"));
                exitThread.start();
            }
        }
    }

    public static void exit(int status, long timeBeforeHalt, TimeUnit timeBeforeHaltUnit) {
        shutdownHaltDelay.setValue(timeBeforeHaltUnit.toMillis(timeBeforeHalt));
        exit(status);
    }

    public static void halt(int status) {
        halt(status, Level.FATAL);
    }

    public static synchronized void halt(int status, Level logLevel) {
        LOGGER.log(logLevel, "JVM halting with status {}; thread dump at halt: {}", status,
                ThreadDumpUtil.takeDumpString());
        // try to give time for the log to be emitted...
        LogManager.shutdown();
        Runtime.getRuntime().halt(status);
    }

    private static class ShutdownWatchdog extends Thread {

        private final Semaphore startSemaphore = new Semaphore(0);

        private ShutdownWatchdog() {
            super("ShutdownWatchdog");
            setDaemon(true);
        }

        @Override
        public void run() {
            startSemaphore.acquireUninterruptibly();
            LOGGER.info("starting shutdown watchdog- system will halt if shutdown is not completed within {} seconds",
                    TimeUnit.MILLISECONDS.toSeconds(shutdownHaltDelay.getValue()));
            try {
                exitThread.join(shutdownHaltDelay.getValue());
                if (exitThread.isAlive()) {
                    try {
                        LOGGER.fatal("shutdown did not complete within configured delay; halting");
                    } finally {
                        ExitUtil.halt(EC_HALT_SHUTDOWN_TIMED_OUT);
                    }
                }
            } catch (Throwable th) { // NOSONAR must catch them all
                ExitUtil.halt(EC_HALT_WATCHDOG_FAILED);
            }
        }

        public void beginWatch() {
            startSemaphore.release();
        }
    }

    private static class ExitThread extends Thread {
        private volatile int status;
        private volatile Throwable callstack;

        ExitThread() {
            super("JVM exit thread");
            setDaemon(true);
        }

        @Override
        public void run() {
            watchdogThread.beginWatch();
            try {
                LOGGER.warn("JVM exiting with status " + status + "; bye!", callstack);
                logShutdownHooks();
            } finally {
                Runtime.getRuntime().exit(status);
            }
        }

        public void setStatus(int status, Throwable callstack) {
            this.status = status;
            this.callstack = callstack;
        }

        private static void logShutdownHooks() {
            try {
                Class clazz = Class.forName("java.lang.ApplicationShutdownHooks");
                Field hooksField = clazz.getDeclaredField("hooks");
                hooksField.setAccessible(true);
                IdentityHashMap hooks = (IdentityHashMap) hooksField.get(null);
                if (hooks != null) {
                    LOGGER.info("the following ({}) shutdown hooks have been registered: {}", hooks::size,
                            hooks::toString);
                }
            } catch (Exception e) {
                LOGGER.debug("ignoring exception trying to log shutdown hooks", e);
            }
        }
    }
}
