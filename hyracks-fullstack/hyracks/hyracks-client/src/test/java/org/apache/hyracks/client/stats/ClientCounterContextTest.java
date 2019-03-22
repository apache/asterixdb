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

package org.apache.hyracks.client.stats;

import java.util.Arrays;

import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.client.stats.impl.ClientCounterContext;
import org.junit.Test;

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
        String[] counters =
                { Counters.MEMORY_USAGE, Counters.MEMORY_MAX, Counters.NETWORK_IO_READ, Counters.NETWORK_IO_WRITE,
                        Counters.SYSTEM_LOAD, Counters.NUM_PROCESSOR, Counters.DISK_READ, Counters.DISK_WRITE };
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
