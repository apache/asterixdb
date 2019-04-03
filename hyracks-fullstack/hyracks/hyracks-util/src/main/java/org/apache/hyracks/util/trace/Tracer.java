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

package org.apache.hyracks.util.trace;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hyracks.util.PidHelper;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/edit
 */
public class Tracer implements ITracer {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final Level TRACE_LOG_LEVEL = Level.forName("TRACER", 570);
    private static final ThreadLocal<DateFormat> DATE_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));

    private final Logger traceLog;
    private long categories;
    private final TraceCategoryRegistry registry;

    private static final long pid = PidHelper.getPid();

    public Tracer(String name, long categories, TraceCategoryRegistry registry) {
        final String traceLoggerName = Tracer.class.getName() + ".Traces." + name;
        LOGGER.info("Initialize Tracer " + traceLoggerName);
        this.traceLog = LogManager.getLogger(traceLoggerName);
        this.categories = categories;
        this.registry = registry;
        final long traceCategory = getRegistry().get(TraceUtils.TRACER);
        instant("Trace-Start", traceCategory, Scope.p, dateTimeStamp());
    }

    public Tracer(String name, String[] categories, TraceCategoryRegistry registry) {
        this(name, ITraceCategoryRegistry.CATEGORIES_ALL, registry);
        setCategories(categories);
    }

    @Override
    public void setCategories(String... categories) {
        LOGGER.info("Set categories for Tracer " + this.traceLog.getName() + " to " + Arrays.toString(categories));
        this.categories = set(categories);
    }

    private long set(String... names) {
        long result = 0;
        for (String name : names) {
            result |= getRegistry().get(name);
        }
        return result;
    }

    public static String dateTimeStamp() {
        return "{\"datetime\":\"" + DATE_FORMAT.get().format(new Date()) + "\"}";
    }

    @Override
    public String toString() {
        return getName() + Long.toHexString(categories);
    }

    @Override
    public String getName() {
        return traceLog.getName();
    }

    @Override
    public TraceCategoryRegistry getRegistry() {
        return registry;
    }

    @Override
    public boolean isEnabled(long cat) {
        return (categories & cat) != 0L;
    }

    @Override
    public long durationB(String name, long cat, String args) {
        if (isEnabled(cat)) {
            Event e = Event.create(name, cat, Phase.B, pid, Thread.currentThread().getId(), null, args, getRegistry());
            traceLog.log(TRACE_LOG_LEVEL, e.toJson());
            return e.tid;
        }
        return -1;
    }

    @Override
    public void durationE(String name, long cat, long tid, String args) {
        if (isEnabled(cat)) {
            Event e = Event.create(name, cat, Phase.E, pid, tid, null, args, getRegistry());
            traceLog.log(TRACE_LOG_LEVEL, e.toJson());
        }
    }

    @Override
    public void durationE(long tid, long cat, String args) {
        if (isEnabled(cat)) {
            Event e = Event.create(null, 0L, Phase.E, pid, tid, null, args, getRegistry());
            traceLog.log(TRACE_LOG_LEVEL, e.toJson());
        }
    }

    @Override
    public void instant(String name, long cat, Scope scope, String args) {
        if (isEnabled(cat)) {
            Event e = Event.create(name, cat, Phase.i, pid, Thread.currentThread().getId(), scope, args, getRegistry());
            traceLog.log(TRACE_LOG_LEVEL, e.toJson());
        }
    }
}
