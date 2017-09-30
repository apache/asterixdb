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

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.util.PidHelper;

/**
 * https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/edit
 */
public class Tracer {

    protected static final Level TRACE_LOG_LEVEL = Level.INFO;

    protected final Logger traceLog;
    protected String[] categories;

    protected static final int pid = PidHelper.getPid();

    public enum Phase {
        // Duration Events
        B, // begin
        E, // end
        // Complete Events
        X,
        // Instant Events
        i,
        // Counter Events
        C,
        // Async Events
        b, // nestable start
        n, // nestable instant
        e, // nestable end
        // Flow Events
        s, // start
        t, // step
        f, // end
        // Object Events
        N, // created
        O, // snapshot
        D // destroyed
    }

    public enum Scope {
        g, // Global scope
        p, // Process scope
        t // Thread scope
    }

    public Tracer(String name, String[] categories) {
        this.traceLog = Logger.getLogger(Tracer.class.getName() + "@" + name);
        this.categories = categories;
    }

    public static Tracer none() {
        return new Tracer("None", new String[0]);
    }

    public static Tracer all() {
        return new Tracer("All", new String[] { "*" });
    }

    @Override
    public String toString() {
        return getName() + Arrays.toString(categories) + (isEnabled() ? "enabled" : "disabled");
    }

    public String getName() {
        return traceLog.getName();
    }

    public boolean isEnabled() {
        return categories.length > 0;
    }

    public long durationB(String name, String cat, String args) {
        Event e = Event.create(name, cat, Phase.B, pid, Thread.currentThread().getId(), null, args);
        traceLog.log(TRACE_LOG_LEVEL, e.toJson());
        return e.tid;
    }

    public void durationE(long tid, String args) {
        Event e = Event.create(null, null, Phase.E, pid, tid, null, args);
        traceLog.log(TRACE_LOG_LEVEL, e.toJson());
    }

    public void instant(String name, String cat, Scope scope, String args) {
        Event e = Event.create(name, cat, Phase.i, pid, Thread.currentThread().getId(), scope, args);
        traceLog.log(TRACE_LOG_LEVEL, e.toJson());
    }
}
