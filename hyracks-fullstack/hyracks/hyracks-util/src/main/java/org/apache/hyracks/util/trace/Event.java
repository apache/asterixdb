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

import java.util.concurrent.TimeUnit;

final class Event {
    private static final long NANOTIME_DELTA_TO_EPOCH =
            System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());

    public final String name;
    public final long cat;
    public final ITracer.Phase ph;
    public final long ts;
    public final long pid;
    public final long tid;
    public final ITracer.Scope scope;
    public final String args;
    public final TraceCategoryRegistry registry;

    private Event(String name, long cat, ITracer.Phase ph, long ts, long pid, long tid, ITracer.Scope scope,
            String args, TraceCategoryRegistry registry) {
        this.name = name;
        this.cat = cat;
        this.ph = ph;
        this.ts = ts;
        this.pid = pid;
        this.tid = tid;
        this.scope = scope;
        this.args = args;
        this.registry = registry;
    }

    private static long timestamp() {
        return (System.nanoTime() - NANOTIME_DELTA_TO_EPOCH) / 1000;
    }

    public static Event create(String name, long cat, ITracer.Phase ph, long pid, long tid, ITracer.Scope scope,
            String args, TraceCategoryRegistry registry) {
        return new Event(name, cat, ph, timestamp(), pid, tid, scope, args, registry);
    }

    public String toJson() {
        return append(new StringBuilder()).toString();
    }

    public StringBuilder append(StringBuilder sb) {
        sb.append("{");
        if (name != null) {
            sb.append("\"name\":\"").append(name).append("\",");
        }
        if (cat != 0L) {
            final String catName = registry.getName(cat);
            sb.append("\"cat\":\"").append(catName).append("\",");
        }
        sb.append("\"ph\":\"").append(ph).append("\",");
        sb.append("\"pid\":\"").append(pid).append("\",");
        sb.append("\"tid\":").append(tid).append(",");
        sb.append("\"ts\":").append(ts);
        if (scope != null) {
            sb.append(",\"s\":\"").append(scope).append("\"");
        }
        if (args != null) {
            if (args.isEmpty()) {
                sb.append(",\"args\":").append("\"\"");
            } else {
                sb.append(",\"args\":").append(args);
            }
        }
        sb.append("}");
        return sb;
    }
}