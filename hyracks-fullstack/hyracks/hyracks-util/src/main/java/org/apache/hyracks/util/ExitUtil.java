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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("squid:S1147")
public class ExitUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final ExitThread exitThread = new ExitThread();

    public static final int EXIT_CODE_SHUTDOWN_TIMED_OUT = 66;
    public static final int EXIT_CODE_WATCHDOG_FAILED = 77;

    private ExitUtil() {
    }

    public static void init() {
        // no-op, the clinit does the work
    }

    public static void exit(int status) {
        exitThread.setStatus(status);
        exitThread.start();
    }

    @SuppressWarnings("squid:S2142") // catch interrupted
    public static void halt(int status) {
        LOGGER.fatal("JVM halting with status " + status + "; bye!", new Throwable("halt stacktrace"));
        try {
            // try to give time for the log to be emitted...
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // ignore
        }
        Runtime.getRuntime().halt(status);
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
