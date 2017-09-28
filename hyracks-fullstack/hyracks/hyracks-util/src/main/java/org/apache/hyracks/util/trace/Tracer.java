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
        I,
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
        Event e = Event.create(name, cat, Phase.B, pid, Thread.currentThread().getId(), args);
        traceLog.log(TRACE_LOG_LEVEL, e.toJson());
        return e.tid;
    }

    public void durationE(long tid, String args) {
        Event e = Event.create(null, null, Phase.E, pid, tid, args);
        traceLog.log(TRACE_LOG_LEVEL, e.toJson());
    }
}

class Event {
    public final String name;
    public final String cat;
    public final Tracer.Phase ph;
    public final long ts;
    public final int pid;
    public final long tid;
    public final String args;

    private Event(String name, String cat, Tracer.Phase ph, long ts, int pid, long tid, String args) {
        this.name = name;
        this.cat = cat;
        this.ph = ph;
        this.ts = ts;
        this.pid = pid;
        this.tid = tid;
        this.args = args;
    }

    private static long timestamp() {
        return System.nanoTime() / 1000;
    }

    public static Event create(String name, String cat, Tracer.Phase ph, int pid, long tid, String args) {
        return new Event(name, cat, ph, timestamp(), pid, tid, args);
    }

    public String toJson() {
        return append(new StringBuilder()).toString();
    }

    public StringBuilder append(StringBuilder sb) {
        sb.append("{");
        if (name != null) {
            sb.append("\"name\":\"").append(name).append("\",");
        }
        if (cat != null) {
            sb.append("\"cat\":\"").append(cat).append("\",");
        }
        sb.append("\"ph\":\"").append(ph).append("\",");
        sb.append("\"pid\":\"").append(pid).append("\",");
        sb.append("\"tid\":").append(tid).append(",");
        sb.append("\"ts\":").append(ts);
        if (args != null) {
            sb.append(",\"args\":").append(args);
        }
        sb.append("}");
        return sb;
    }
}
