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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.util.PidHelper;

/**
 * https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/edit
 */
public class Tracer implements ITracer {

    public static final Logger LOGGER = Logger.getLogger(Tracer.class.getName());

    protected static final Level TRACE_LOG_LEVEL = Level.INFO;
    protected static final String CAT = "Tracer";
    protected static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    protected final Logger traceLog;
    protected String[] categories;

    protected static final int pid = PidHelper.getPid();

    public Tracer(String name, String[] categories) {
        final String traceLoggerName = Tracer.class.getName() + "@" + name;
        LOGGER.info("Initialize Tracer " + traceLoggerName + " " + Arrays.toString(categories));
        this.traceLog = Logger.getLogger(traceLoggerName);
        this.categories = categories;
        instant("Trace-Start", CAT, Scope.p, dateTimeStamp());
    }

    public static String dateTimeStamp() {
        synchronized (DATE_FORMAT) {
            return "{\"datetime\":\"" + DATE_FORMAT.format(new Date()) + "\"}";
        }
    }

    public static final Tracer ALL = new Tracer("All", new String[] { "*" });

    @Override
    public String toString() {
        return getName() + Arrays.toString(categories) + (isEnabled() ? "enabled" : "disabled");
    }

    @Override
    public String getName() {
        return traceLog.getName();
    }

    @Override
    public boolean isEnabled() {
        return categories.length > 0;
    }

    @Override
    public long durationB(String name, String cat, String args) {
        Event e = Event.create(name, cat, Phase.B, pid, Thread.currentThread().getId(), null, args);
        traceLog.log(TRACE_LOG_LEVEL, e.toJson());
        return e.tid;
    }

    @Override
    public void durationE(long tid, String args) {
        Event e = Event.create(null, null, Phase.E, pid, tid, null, args);
        traceLog.log(TRACE_LOG_LEVEL, e.toJson());
    }

    @Override
    public void instant(String name, String cat, Scope scope, String args) {
        Event e = Event.create(name, cat, Phase.i, pid, Thread.currentThread().getId(), scope, args);
        traceLog.log(TRACE_LOG_LEVEL, e.toJson());
    }
}
