/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.test.support;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.Counter;

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