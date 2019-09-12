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
package org.apache.hyracks.test.support;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.com.job.profiling.counters.Counter;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;

public class CounterContext implements ICounterContext {
    private final String contextName;
    private final Map<String, Counter> counterMap;

    public CounterContext(String name) {
        this.contextName = name;
        counterMap = new HashMap<String, Counter>();
    }

    @Override
    public synchronized ICounter getCounter(String counterName, boolean create) {
        Counter counter = counterMap.get(counterName);
        if (counter == null && create) {
            counter = new Counter(contextName + "." + counterName);
            counterMap.put(counterName, counter);
        }
        return counter;
    }

    public synchronized void dump(Map<String, Long> dumpMap) {
        for (Counter c : counterMap.values()) {
            dumpMap.put(c.getName(), c.get());
        }
    }
}
