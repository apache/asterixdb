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
package org.apache.hyracks.api.com.job.profiling.counters;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.job.profiling.counters.ICounter;

@SuppressWarnings("squid:S1700")
public class Counter implements ICounter {
    private static final long serialVersionUID = -3935601595055562080L;

    private final String name;
    private final AtomicLong counter;

    public Counter(String name) {
        this.name = name;
        counter = new AtomicLong();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long update(long delta) {
        return counter.addAndGet(delta);
    }

    @Override
    public long set(long value) {
        long oldValue = counter.get();
        counter.set(value);
        return oldValue;
    }

    @Override
    public long get() {
        return counter.get();
    }
}
