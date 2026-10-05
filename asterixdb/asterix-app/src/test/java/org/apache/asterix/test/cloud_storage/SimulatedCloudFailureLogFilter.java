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
package org.apache.asterix.test.cloud_storage;

import static org.apache.asterix.cloud.clients.UnstableCloudClient.SIMULATED_ERROR_MESSAGE;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.apache.asterix.cloud.clients.UnstableCloudClient;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

/**
 * Drops log events caused by the failures {@link UnstableCloudClient} injects. Every retried request and every
 * aborted job logs the simulated failure with its stack trace, burying anything of interest in the unstable cloud
 * tests' logs. Installed as a configuration-wide filter so events are dropped before any logger or appender sees
 * them; events without a simulated failure are left to the regular configuration.
 */
public final class SimulatedCloudFailureLogFilter extends AbstractFilter {

    private static final SimulatedCloudFailureLogFilter INSTANCE = new SimulatedCloudFailureLogFilter();

    private SimulatedCloudFailureLogFilter() {
        super(Result.DENY, Result.NEUTRAL);
    }

    public static void install() {
        LoggerContext.getContext(false).getConfiguration().addFilter(INSTANCE);
    }

    public static void uninstall() {
        LoggerContext.getContext(false).getConfiguration().removeFilter(INSTANCE);
    }

    @Override
    public Result filter(LogEvent event) {
        return result(isSimulated(event.getThrown()) || isSimulated(event.getMessage()));
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
        return result(isSimulated(t) || isSimulated(msg));
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Object msg, Throwable t) {
        return result(isSimulated(t) || (msg instanceof String && ((String) msg).contains(SIMULATED_ERROR_MESSAGE)));
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, String msg) {
        return result(msg != null && msg.contains(SIMULATED_ERROR_MESSAGE));
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, String msg, Object... params) {
        // parameterized calls pass the throwable as the trailing parameter rather than separately
        return result((msg != null && msg.contains(SIMULATED_ERROR_MESSAGE)) || anySimulated(params));
    }

    private Result result(boolean simulated) {
        return simulated ? onMatch : onMismatch;
    }

    private static boolean isSimulated(Message msg) {
        if (msg == null) {
            return false;
        }
        if (isSimulated(msg.getThrowable()) || anySimulated(msg.getParameters())) {
            return true;
        }
        String formatted = msg.getFormattedMessage();
        return formatted != null && formatted.contains(SIMULATED_ERROR_MESSAGE);
    }

    private static boolean anySimulated(Object[] params) {
        if (params != null) {
            for (Object param : params) {
                if (param instanceof Throwable && isSimulated((Throwable) param)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isSimulated(Throwable t) {
        return t != null && isSimulated(t, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    // the simulated failure may be wrapped arbitrarily deep, or rethrown only as a message once it crosses a job
    // boundary, so the whole cause and suppressed graph is searched
    private static boolean isSimulated(Throwable t, Set<Throwable> visited) {
        if (t == null || !visited.add(t)) {
            return false;
        }
        String message = t.getMessage();
        if (message != null && message.contains(SIMULATED_ERROR_MESSAGE)) {
            return true;
        }
        for (Throwable suppressed : t.getSuppressed()) {
            if (isSimulated(suppressed, visited)) {
                return true;
            }
        }
        return isSimulated(t.getCause(), visited);
    }
}
