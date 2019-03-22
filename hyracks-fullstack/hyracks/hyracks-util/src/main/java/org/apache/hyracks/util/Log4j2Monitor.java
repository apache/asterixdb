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

import java.io.IOException;
import java.io.Writer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

public class Log4j2Monitor {

    private static final List<String> logs = new ArrayList<>();

    private Log4j2Monitor() {
    }

    public static void start() {
        final LoggerContext context = LoggerContext.getContext(false);
        final Appender appender = WriterAppender.createAppender(null, null, new LogWriter(), "MEMORY", false, true);
        appender.start();
        final Configuration config = context.getConfiguration();
        config.addAppender(appender);
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(appender, null, null);
        }
        config.getRootLogger().addAppender(appender, Level.ALL, null);
        context.updateLoggers();
    }

    public static long count(String message) {
        return logs.stream().filter(log -> log.contains(message)).count();
    }

    public static void reset() {
        logs.clear();
    }

    public static List<String> getLogs() {
        return logs;
    }

    private static class LogWriter extends Writer {

        @Override
        public void write(char[] buffer, int off, int len) {
            logs.add(CharBuffer.wrap(buffer, off, len).toString());
        }

        @Override
        public void flush() {
            // no op
        }

        @Override
        public void close() throws IOException {
            // no op
        }
    }
}
