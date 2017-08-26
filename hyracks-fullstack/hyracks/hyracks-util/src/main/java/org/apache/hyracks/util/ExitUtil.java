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

import java.util.logging.Logger;

@SuppressWarnings("squid:S1147")
public class ExitUtil {

    private static final Logger LOGGER = Logger.getLogger(ExitUtil.class.getName());

    private static final ExitThread exitThread = new ExitThread();

    private ExitUtil() {
    }

    public static void init() {
        // no-op, the clinit does the work
    }

    public static void exit(int status) {
        exitThread.setStatus(status);
        exitThread.start();
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
