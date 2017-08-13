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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * RuntimeLogsMonitor is used to store the generated runtime logs in-memory
 * and provides API to search the stored logs.
 */
public class RuntimeLogsMonitor {

    private static final InMemoryHandler IN_MEMORY_HANDLER = new InMemoryHandler();
    private static final List<Logger> MONITORED_LOGGERS = new ArrayList<>();
    private static List<LogRecord> logs;

    private RuntimeLogsMonitor() {
        reset();
    }

    /**
     * Starts monitoring the logger by storing its generated logs in-memory. By default
     * only logs with level WARNING and above are stored
     *
     * @param loggerName
     */
    public static void monitor(String loggerName) {
        final Logger logger = Logger.getLogger(loggerName);
        for (Handler handler : logger.getHandlers()) {
            if (handler == IN_MEMORY_HANDLER) {
                return;
            }
        }
        MONITORED_LOGGERS.add(logger);
        Logger.getLogger(loggerName).addHandler(IN_MEMORY_HANDLER);
    }

    /**
     * Discards any stored logs
     */
    public static void reset() {
        logs = new ArrayList<>();
    }

    /**
     * Calculates the count based on {@code logRecord} level and message.
     * if any stored log has the same level as {@code logRecord} and
     * the log's message contains {@code logRecord} message, it is considered
     * as an occurrence
     *
     * @param logRecord
     * @return The number of found logs that match {@code logRecord}
     */
    public static long count(LogRecord logRecord) {
        return logs.stream()
                .filter(storedLog -> storedLog.getLevel().equals(logRecord.getLevel()) && storedLog.getMessage()
                        .contains(logRecord.getMessage())).count();
    }

    /**
     * Sets the stored logs minimum level
     *
     * @param lvl
     */
    public static void setLevel(Level lvl) {
        IN_MEMORY_HANDLER.setLevel(lvl);
    }

    /**
     * Stops monitoring any monitored loggers and discards any
     * stored logs
     */
    public static void stop() {
        for (Logger logger : MONITORED_LOGGERS) {
            logger.removeHandler(IN_MEMORY_HANDLER);
        }
        reset();
        MONITORED_LOGGERS.clear();
    }

    private static class InMemoryHandler extends Handler {
        private InMemoryHandler() {
            super.setLevel(Level.WARNING);
            setFilter(record -> record.getLevel().intValue() >= getLevel().intValue());
        }

        @Override
        public void publish(LogRecord lr) {
            if (isLoggable(lr)) {
                logs.add(lr);
            }
        }

        @Override
        public void flush() {
            // nothing to flush
        }

        @Override
        public void close() {
            // nothing to close
        }
    }
}
