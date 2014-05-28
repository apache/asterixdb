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

package edu.uci.ics.hyracks.client.stats;

import java.util.Arrays;

import org.junit.Test;

import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.client.stats.impl.ClientCounterContext;

public class ClientCounterContextTest {

    @Test
    public void test() throws Exception {
        HyracksUtils.init();
        String[] ncs = new String[] { "nc1", "nc2" };
        ClientCounterContext ccContext = new ClientCounterContext("localhost", 16001, Arrays.asList(ncs));
        ccContext.resetAll();
        synchronized (this) {
            wait(20000);
        }
        String[] counters = { Counters.MEMORY_USAGE, Counters.MEMORY_MAX, Counters.NETWORK_IO_READ,
                Counters.NETWORK_IO_WRITE, Counters.SYSTEM_LOAD, Counters.NUM_PROCESSOR, Counters.DISK_READ,
                Counters.DISK_WRITE };
        for (String counterName : counters) {
            ICounter counter = ccContext.getCounter(counterName, false);
            System.out.println(counterName + ": " + counter.get());
        }
        for (String slave : ncs) {
            for (String counterName : counters) {
                ICounter counter = ccContext.getCounter(slave, counterName, false);
                System.out.println(slave + " " + counterName + ": " + counter.get());
            }
        }
        ccContext.stop();
        HyracksUtils.deinit();
    }
}
